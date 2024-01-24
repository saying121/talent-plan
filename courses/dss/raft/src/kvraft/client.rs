use std::{
    fmt,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use futures::{executor::block_on, FutureExt};
use futures_timer::Delay;

use crate::{proto::kvraftpb::*, THREAD_POOL};

enum Op {
    Put {
        key:   String,
        value: String,
    },
    Append {
        key:   String,
        value: String,
    },
}

pub struct Clerk {
    pub name:    String,
    pub clients: Vec<KvClient>, // You will have to modify this struct.

    sequence:  AtomicU64,
    leader_id: AtomicUsize,
}

impl Clerk {
    fn put_append(&self, op: Op) {
        let (key, value, append) = match op {
            Op::Append { key, value } => (key, value, true),
            Op::Put { key, value } => (key, value, false),
        };
        let args = PutAppendArgs {
            command_id: Some(self.command_id()),
            command:    Some(PutAppendCommand { key, value, append }),
        };
        let mut clients = self
            .clients
            .iter()
            .enumerate()
            .cycle()
            .skip(
                self.leader_id
                    .load(Ordering::Relaxed),
            );

        'client: loop {
            let (id, client) = clients
                .next()
                .expect("Clerk loop put append");
            loop {
                let mut rpc = client.put_append(&args).fuse();
                let reply = block_on(async {
                    futures::select! {
                        reply = rpc => Some(reply),
                        _ = Delay::new(Self::RPC_TIMEOUT).fuse() => None,
                    }
                });
                if reply.is_none() {
                    THREAD_POOL.spawn_ok(async { _ = rpc.await });
                    continue 'client;
                }
                let reply = match reply.expect("Clerk inner loop put appen") {
                    Ok(reply) if reply.is_leader => reply,
                    _ => continue 'client,
                };
                self.leader_id
                    .store(id, Ordering::Relaxed);
                if reply.err.is_empty() {
                    // break 'client reply;
                    break 'client;
                }
            }
        }
    }
    fn command_id(&self) -> CommandId {
        CommandId {
            clerk_id:  self.name.clone(),
            clerk_seq: self
                .sequence
                .fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk")
            .field("name", &self.name)
            .finish()
    }
}

impl Clerk {
    const RPC_TIMEOUT: Duration = Duration::from_millis(500);

    pub fn new(name: String, clients: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        Self {
            name,
            clients,
            sequence: 1.into(),
            leader_id: 0.into(),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = GetArgs {
            command_id: Some(self.command_id()),
            command:    Some(GetCommand { key }),
        };
        {
            let mut clients = self
                .clients
                .iter()
                .enumerate()
                .cycle()
                .skip(
                    self.leader_id
                        .load(Ordering::Relaxed),
                );
            'client: loop {
                let (id, client) = clients
                    .next()
                    .expect("Cerk loop get");
                loop {
                    let mut rpc = client.get(&args).fuse();
                    let reply = block_on(async {
                        futures::select! {
                            reply = rpc => Some(reply),
                            _ = Delay::new(Clerk::RPC_TIMEOUT).fuse() => None,
                        }
                    });
                    if reply.is_none() {
                        THREAD_POOL.spawn_ok(async { _ = rpc.await });
                        continue 'client;
                    }
                    let reply = match reply.expect("Cerk inner loop get") {
                        Ok(reply) if reply.is_leader => reply,
                        _ => continue 'client,
                    };
                    self.leader_id
                        .store(id, Ordering::Relaxed);
                    if reply.err.is_empty() {
                        break 'client reply;
                    }
                }
            }
        }
        .value
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    // fn put_append(&self, op: Op) {
    //     // You will have to modify this function.
    //     crate::your_code_here(op)
    // }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put { key, value })
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append { key, value })
    }
}
