use anyhow::{anyhow, Result};
use clap::Parser;
use io_uring::{
    cqueue::CompletionQueue,
    opcode,
    squeue::{self, Entry, SubmissionQueue},
    types, IoUring,
};
use std::str;

use nix::{
    sched::{self, CpuSet},
    unistd::Pid,
};
use std::{
    collections::vec_deque::VecDeque, fs::File, io::ErrorKind, net::UdpSocket,
    os::unix::io::AsRawFd, thread,
};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    /// Set IOSQE_ASNYC flag on SQEs
    #[clap(short, long = "async")]
    async_work: bool,
    #[clap(short, long, default_value = "0")]
    sqes: u32,
    #[clap(short, long, default_value = "0")]
    max_unbounded_workers: u32,
    #[clap(short = 'r', long, default_value = "1")]
    num_rings: usize,
    #[clap(short = 't', long, default_value = "1")]
    num_threads: usize,
    #[clap(short, long, default_value = "0")]
    cpu: Vec<usize>,
}

const MAX_SQES: u32 = 4096;

fn main() -> Result<()> {
    let args = Opts::parse();
    if args.num_threads > 1 {
        let threads: Vec<_> = (0..args.num_threads)
            .map(|_| thread::spawn(do_work))
            .collect();
        for t in threads {
            t.join().unwrap()?;
        }
    } else {
        do_work()?
    }
    Ok(())
}

fn do_work() -> Result<()> {
    let args = Opts::parse();
    println!("{:?}", thread::current().id());
    let sink = UdpSocket::bind("127.0.0.1:3000")?;
    let sink_fd = types::Fd(sink.as_raw_fd());

    let mut backlog = VecDeque::new();
    let mut rd_buf = [0u8; 1024];
    let rd_op = opcode::Read::new(sink_fd, &mut rd_buf as _, rd_buf.len() as _);

    let rd_flags = if args.async_work {
        squeue::Flags::ASYNC
    } else {
        squeue::Flags::empty()
    };

    let rd_sqe = rd_op.build().flags(rd_flags);

    let mut cpu_iter = args
        .cpu
        .iter()
        .map(|c| {
            let mut cs = CpuSet::new();
            cs.set(*c).unwrap();
            cs
        })
        .cycle();

    let mut rings: Vec<IoUring> = Vec::with_capacity(args.num_rings);
    for _ in 0..args.num_rings {
        rings.push(create_ring(MAX_SQES, args.max_unbounded_workers)?);
    }

    let cpu_set = sched::sched_getaffinity(Pid::from_raw(0))?;

    let num_sqes = if args.sqes > 0 { args.sqes } else { MAX_SQES };
    for r in &mut rings {
        let c = cpu_iter.next().unwrap();
        sched::sched_setaffinity(Pid::from_raw(0), &c)?;

        fill_sq(&mut r.submission(), &rd_sqe, num_sqes)?;
        r.submit()?;
    }
    sched::sched_setaffinity(Pid::from_raw(0), &cpu_set)?;

    loop {
        for r in &mut rings {
            let (submitter, mut sq, mut cq) = r.split();
            match submitter.submit_and_wait(1) {
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(anyhow!(e)),
                Ok(_) => (),
            }

            cq.sync();

            loop {
                if sq.is_full() {
                    match submitter.submit() {
                        Ok(_) => (),
                        Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                        Err(err) => return Err(err.into()),
                    }
                }
                sq.sync();
                match backlog.pop_front() {
                    Some(sqe) => unsafe {
                        let _ = sq.push(&sqe);
                    },
                    None => break,
                }
            }

            fill_sq(&mut sq, &rd_sqe, num_sqes)?;

            for cqe in &mut cq {
                let res = cqe.result();
                let index = cqe.user_data() as usize;

                let cur = str::from_utf8(&rd_buf[..res as usize])?;
                println!("{:?}, {:?}", cur, index);
            }
        }
    }
}

fn create_ring(max_sqes: u32, max_unbounded_workers: u32) -> Result<IoUring> {
    let ring = IoUring::new(max_sqes)?;
    let sub = ring.submitter();

    let mut max_workers: [u32; 2] = [0, max_unbounded_workers];
    sub.register_iowq_max_workers(&mut max_workers)?;

    Ok(ring)
}

fn fill_sq(sq: &mut SubmissionQueue, sqe: &Entry, num_sqes: u32) -> Result<()> {
    let mut i = 0;

    sq.sync();
    while !sq.is_full() && i < num_sqes {
        unsafe {
            sq.push(sqe)?;
        }
        i += 1;
    }
    sq.sync();

    Ok(())
}

fn drain_cq(cq: &mut CompletionQueue, rd_buf: &mut [u8; 1024]) -> Result<()> {
    cq.sync();
    for r in cq.into_iter() {
        let cur = str::from_utf8(&rd_buf[0..r.result() as usize])?;
        println!("{:?}", cur);
    }
    cq.sync();
    Ok(())
}

#[test]
fn default_args() {
    let args = Opts::parse();
    println!("{:?}", args);
    assert_eq!(args.async_work, false);
    assert_eq!(args.sqes, 0);
    assert_eq!(args.max_unbounded_workers, 0);
    assert_eq!(args.num_rings, 1);
    assert_eq!(args.num_threads, 1);
    assert_eq!(args.cpu, vec![0]);
}
