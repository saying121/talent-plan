#![feature(lazy_cell)]

use std::sync::LazyLock;

use futures::executor::ThreadPool;

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

/// A place holder for suppressing unused_variables warning.
fn your_code_here<T>(_: T) -> ! {
    log::info!("---------------- no implement");
    unimplemented!()
}

static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| ThreadPool::new().unwrap());
