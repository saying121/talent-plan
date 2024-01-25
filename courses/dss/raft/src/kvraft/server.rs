use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver},
        oneshot::{self, Sender},
    },
    FutureExt, StreamExt,
};
use futures_timer::Delay;

use crate::{
    proto::kvraftpb::*,
    raft::{self, ApplyMsg},
    THREAD_POOL,
};

pub struct KvServer {
    pub rf: raft::Node,
    // snapshot if log grows this big
    // maxraftstate: Option<usize>, // Your definitions here.
    state:  Option<State>,

    replyer: Arc<Mutex<HashMap<u64, Replyer>>>,
}
#[derive(Debug)]
pub struct State {
    store:     HashMap<String, String>,
    clerk_seq: HashMap<String, u64>,

    max_raft_state: Option<usize>,
    apply_ch:       UnboundedReceiver<ApplyMsg>,
}

impl State {
    fn snapshot(&mut self, data: &[u8]) {
        if data.is_empty() {
            self.store.clear();
            self.clerk_seq.clear();
            return;
        }
        let snapshot: Snapshot = labcodec::decode(data).expect("State snapshot");
        self.store = snapshot.store;
        self.clerk_seq = snapshot.clerk_seq;
    }
}

#[derive(Debug)]
pub struct Replyer {
    term:     u64,
    reply_tx: Sender<Option<String>>,
}

#[derive(Message)]
pub struct IdentCommand {
    #[prost(message, optional, tag = "1")]
    command_id: Option<CommandId>,
    #[prost(oneof = "Command", tags = "2, 3")]
    command:    Option<Command>,
}

#[derive(Oneof)]
enum Command {
    #[prost(message, tag = "2")]
    Get(GetCommand),
    #[prost(message, tag = "3")]
    PutAppend(PutAppendCommand),
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let mut state = State {
            store: HashMap::new(),
            clerk_seq: HashMap::new(),
            max_raft_state: maxraftstate,
            apply_ch,
        };
        state.snapshot(&persister.snapshot());
        let rf = raft::Raft::new(servers, me, persister, tx);

        Self {
            rf:      raft::Node::new(rf),
            // maxraftstate,
            state:   Some(state),
            replyer: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn spawn_daemon(&mut self) {
        let raft = self.rf.clone();
        let mut state = self
            .state
            .take()
            .expect("KvServer spawn daemon");

        let replyer = Arc::clone(&self.replyer);
        THREAD_POOL.spawn_ok(async move {
            while let Some(message) = state.apply_ch.next().await {
                if let ApplyMsg::Snapshot { data, term, index } = message {
                    let condition = raft.cond_install_snapshot(term, index, &data);
                    assert!(condition);
                    state.snapshot(&data);
                    continue;
                }

                let (data, index) = match message {
                    ApplyMsg::Command { data, index } => (data, index),
                    ApplyMsg::Snapshot { .. } => unreachable!(),
                };

                let ident_command: IdentCommand =
                    labcodec::decode(&data).expect("KvServer while loop");

                let command_id = ident_command
                    .command_id
                    .expect("KvServer while loop command id");
                let sequence = state
                    .clerk_seq
                    .entry(command_id.clerk_id)
                    .or_default();

                let apply = command_id.clerk_seq > *sequence;
                if apply {
                    *sequence = command_id.clerk_seq;
                }

                let command = ident_command
                    .command
                    .expect("KvServer while loop command.");
                let reply = if let Command::Get(command) = command {
                    Some(
                        state
                            .store
                            .get(&command.key)
                            .cloned()
                            .unwrap_or_default(),
                    )
                }
                else if apply {
                    let command = match command {
                        Command::Get(_) => unreachable!(),
                        Command::PutAppend(cmd) => cmd,
                    };
                    let value = state
                        .store
                        .entry(command.key)
                        .or_default();
                    if command.append {
                        value.push_str(&command.value);
                    }
                    else {
                        *value = command.value;
                    }
                    None
                }
                else {
                    None
                };

                let raft_state = raft.get_state();
                let replyer = replyer
                    .lock()
                    .expect("KvServer while loop replyer")
                    .remove(&index);

                if raft_state.is_leader()
                    && matches!(&replyer, Some(replyer) if replyer.term == raft_state.term())
                {
                    _ = replyer
                        .expect("KvServer raft_state is_leader")
                        .reply_tx
                        .send(reply);
                }

                if !matches!(state.max_raft_state, Some(size) if size <= raft.size()) {
                    continue;
                }

                let snapshot = Snapshot {
                    store:     state.store.clone(),
                    clerk_seq: state.clerk_seq.clone(),
                };

                let mut data = Vec::new();
                labcodec::encode(&snapshot, &mut data).expect("KvServer labcodec encode");

                raft.snapshot(index, &data);
            }
        })
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    server: Arc<KvServer>,
}

impl Node {
    const REPLY_TIMEOUT: Duration = Duration::from_millis(500);

    pub fn new(mut kv: KvServer) -> Node {
        // Your code here.
        kv.spawn_daemon();
        Self { server: Arc::new(kv) }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        self.server.rf.kill()
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.server.rf.is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, args: GetArgs) -> labrpc::Result<GetReply> {
        let v = {
            let result = self
                .server
                .rf
                .start(&IdentCommand {
                    command_id: args.command_id,
                    command:    Some(Command::Get(args.command.unwrap())),
                });
            if let Err(error) = result {
                let mut reply = GetReply::new(false, "", "");
                reply.is_leader = error != raft::errors::Error::NotLeader;
                reply.err = error.to_string();
                return Ok(reply);
            }
            let (index, term) = result.expect("impl KvService for Node index,term");
            let (reply_tx, mut reply_rx) = oneshot::channel();
            let replyer = Replyer { term, reply_tx };
            self.server
                .replyer
                .lock()
                .expect("impl KvService for Node replyer")
                .insert(index, replyer);
            let reply = futures::select! {
                reply = reply_rx => reply,
                _ = Delay::new(Self::REPLY_TIMEOUT).fuse()=>return Err(labrpc::Error::Timeout),
            };
            match reply {
                Ok(reply) => reply,
                Err(error) => {
                    return Err(labrpc::Error::Recv(error));
                },
            }
        }
        .expect("impl KvService for Node end");
        Ok(GetReply::new(true, &v, ""))
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendArgs) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        {
            let result = self
                .server
                .rf
                .start(&IdentCommand {
                    command_id: arg.command_id,
                    command:    Some(Command::PutAppend(arg.command.unwrap())),
                });
            if let Err(error) = result {
                let mut reply = PutAppendReply::new(false, "");
                reply.is_leader = error != raft::errors::Error::NotLeader;
                reply.err = error.to_string();
                return Ok(reply);
            }
            let (index, term) = result.expect("impl KvService for Node index,term");
            let (reply_tx, mut reply_rx) = oneshot::channel();
            let replyer = Replyer { term, reply_tx };
            self.server
                .replyer
                .lock()
                .expect("impl KvService for Node replyer end")
                .insert(index, replyer);
            let reply = futures::select! {
                reply = reply_rx => reply,
                _ = Delay::new(Self::REPLY_TIMEOUT).fuse() => return Err(labrpc::Error::Timeout)
            };

            match reply {
                Ok(reply) => reply,
                Err(error) => {
                    return Err(labrpc::Error::Recv(error));
                },
            }
        };

        Ok(PutAppendReply::new(true, ""))
    }
}

impl GetReply {
    fn new(is_leader: bool, value: &str, error: &str) -> Self {
        Self {
            is_leader,
            err: error.into(),
            value: value.into(),
        }
    }
}
impl PutAppendReply {
    fn new(is_leader: bool, error: &str) -> Self {
        Self { is_leader, err: error.into() }
    }
}

#[derive(Message)]
struct Snapshot {
    #[prost(map = "string, string", tag = "1")]
    store: HashMap<String, String>,

    #[prost(map = "string, uint64", tag = "2")]
    clerk_seq: HashMap<String, u64>,
}
