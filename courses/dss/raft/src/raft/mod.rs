use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::{
        mpsc::{sync_channel, Receiver},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
    u64,
};

use futures::{
    channel::mpsc::UnboundedSender,
    future::FutureExt,
    sink::SinkExt,
    stream::{FuturesUnordered, StreamExt},
};
use futures_timer::Delay;
use labrpc::Result as RpcResult;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::{errors::*, persister::*};
use crate::{proto::raftpb::*, THREAD_POOL};

const HEARTBEAT_PERIOD: Duration = Duration::from_millis(80);

fn generate_election_timeout() -> Duration {
    Duration::from_millis(240 + rand::random::<u64>() % 240)
}

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug)]
pub enum ApplyMsg {
    Command {
        data:  Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data:  Vec<u8>,
        term:  u64,
        index: u64,
    },
}

#[derive(Message)]
struct Persistence {
    /// 服务器接收到的最近的任期号(term)(启动时初使化为0，单调递增)
    #[prost(uint64, tag = "1")]
    current_term: u64,
    /// 当前任期内收到的候选者Id(candidateId)(如果没有就是null)
    #[prost(message, tag = "2")]
    voted_for:    Option<u64>,
    /// 日志条目，每一条包括可供状态机执行的命令，收到时的任期号(第一个任期号为1)
    #[prost(message, tag = "3")]
    log:          Option<Log>,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term:      u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Message, Clone)]
struct Log {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<LogEntry>,
    #[prost(uint64, tag = "2")]
    offset:  u64,
}

impl Log {
    fn new() -> Self {
        Self {
            entries: vec![LogEntry::default()],
            offset:  0,
        }
    }
    fn first(&self) -> &LogEntry {
        &self.entries[0]
    }
    fn last(&self) -> &LogEntry {
        self.entries.last().unwrap()
    }
    fn len(&self) -> u64 {
        self.entries.len() as u64 + self.offset
    }
    fn get(&self, index: u64) -> Option<&LogEntry> {
        self.entries
            .get(index.checked_sub(self.offset)? as usize)
    }
    fn push(&mut self, entry: LogEntry) {
        assert!(entry.term >= self.last().term);
        self.entries.push(entry);
    }
    fn append(&mut self, other: &mut Vec<LogEntry>) {
        if other.is_empty() {
            return;
        }

        assert!(other[0].term >= self.last().term);
        self.entries.append(other)
    }
    fn truncate(&mut self, len: u64) {
        assert!(len > self.offset);
        self.entries
            .truncate((len - self.offset) as usize);
    }
    fn tail(&self, from: u64) -> impl Iterator<Item = &LogEntry> {
        assert!(from > self.offset);
        self.entries
            .iter()
            .skip((from - self.offset) as usize)
    }
    fn back(&self, with: u64) -> Option<u64> {
        let next = self
            .entries
            .binary_search_by_key(&(with + 1), |entry| entry.term)
            .unwrap_or_else(|index| index);

        if next <= 1 {
            return None;
        }

        let index = next - 1;
        (self.entries[index].term == with).then(|| index as u64 + self.offset)
    }
    fn offset(&mut self, to: u64) {
        assert!((self.offset + 1..self.len()).contains(&to));
        self.entries
            .drain(0..(to - self.offset) as usize);

        self.entries[0].data.clear();
        self.offset = to;
    }
    fn reset(&mut self, term: u64, index: u64) {
        assert!(term >= self.first().term);
        self.entries = vec![LogEntry::new(Vec::new(), term)];

        assert!(index > self.offset);
        self.offset = index;
    }
}

#[derive(PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    // 要知道所有节点
    peers:        Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister:    Box<dyn Persister>,
    // this peer's index into peers[]
    me:           usize,
    state:        Arc<State>, // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    current_term: u64,
    voted_for:    Option<usize>,
    log:          Log,

    // 所有服务器上的不稳定状态
    /// 已知的被提交的最大日志记录索引值(初始为0，单调递增)
    commit_index: u64,
    /// 被状态机执行的最大日志索引号(初始为0，单调递增)
    last_applied: u64,

    role:      Role,
    /// 检查这个时间点,判断是否超时
    last_sign: Instant,

    /// leader 发送committed 信息
    apply_ch: UnboundedSender<ApplyMsg>,

    weak: Weak<Mutex<Self>>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    ///
    /// 服务或测试程序希望创建一个Raft服务器。
    /// 所有Raft服务器（包括这个服务器）的端口都在peers中。
    /// 这个服务器的端口是peers[me]。
    /// 所有服务器的peers数组顺序相同。
    /// persister是这个服务器保存其持久状态的位置，并且最初保存最近的保存状态（如果有的话）。
    /// apply_ch是一个通道，测试程序或服务期望Raft向其发送ApplyMsg消息。此方法必须快速返回。
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Self {
            peers,
            persister,
            me,
            state: Arc::default(),
            log: Log::new(),

            current_term: 0,
            voted_for: None,

            commit_index: 0,
            last_applied: 0,

            role: Role::Follower,
            last_sign: Instant::now(),

            apply_ch,
            weak: Weak::new(),
        };
        if raft_state.is_empty() {
            return rf;
        }

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        if rf.log.offset == 0 {
            return rf;
        }
        rf.snapshot(rf.persister.snapshot(), rf.log.first().term, rf.log.offset);

        rf
    }
    fn spawn_follower_daemon(&mut self) {
        self.last_sign = Instant::now();

        let weak = Weak::clone(&self.weak);
        let current_term = self.current_term;

        THREAD_POOL.spawn_ok(async move {
            let election_timeout = generate_election_timeout();
            loop {
                let Some(raft) = weak.upgrade()
                else {
                    return;
                };

                let elapsed = {
                    let mut raft = {
                        let raft = raft.lock().unwrap();
                        assert!(raft.current_term >= current_term);

                        let role = Role::Follower;
                        // term更新后就可以退出
                        // 切换成功了,role不是Follower就退出
                        if raft.current_term > current_term || raft.role != role {
                            return;
                        }
                        raft
                    };
                    let elapsed = raft.last_sign.elapsed();
                    // 选举超时后进入candidate, 开始选票
                    if elapsed > election_timeout {
                        raft.transfer(current_term + 1, Role::Candidate);
                        return;
                    }
                    elapsed
                };
                Delay::new(election_timeout - elapsed).await;
            }
        })
    }
    fn spawn_candidate_daemon(&mut self) {
        self.voted_for = Some(self.me);

        let total = self.peers.len();
        if total == 1 {
            self.transfer(self.current_term, Role::Leader);
            return;
        }

        type Rpc = Pin<Box<dyn Future<Output = RpcResult<RequestVoteReply>> + Send>>;
        struct Guard {
            inner: FuturesUnordered<Rpc>,
        }

        impl Drop for Guard {
            fn drop(&mut self) {
                let rpcs = mem::replace(&mut self.inner, FuturesUnordered::new());
                THREAD_POOL.spawn_ok(async { _ = rpcs.collect::<Vec<_>>().await })
            }
        }
        let args = RequestVoteArgs {
            term:           self.current_term,
            candidate_id:   self.me as u64,
            last_log_term:  self.log.last().term,
            last_log_index: self.log.len() - 1,
        };
        let rpcs = self
            .other_peers()
            .map(|(_, peer)| peer.request_vote(&args))
            .collect();
        let mut rpcs = Guard { inner: rpcs };
        let weak = Weak::clone(&self.weak);
        let current_term = self.current_term;
        THREAD_POOL.spawn_ok(async move {
            let mut election_timer = Delay::new(generate_election_timeout()).fuse();
            let mut votes = 1;
            let majority = total / 2 + 1;
            loop {
                let Some(raft) = weak.upgrade()
                else {
                    return;
                };
                let mut rpc = rpcs.inner.select_next_some();
                let reply = futures::select! {
                    rpy = rpc => Some(rpy),
                    _ = election_timer => None,
                };
                if reply.is_none() {
                    {
                        let mut raft = raft.lock().unwrap();
                        assert!(raft.current_term >= current_term);
                        if raft.current_term > current_term || raft.role != Role::Candidate {
                            return;
                        }
                        raft.transfer(current_term + 1, Role::Candidate);
                    }
                    return;
                }
                let reply = {
                    let reply = match reply.unwrap() {
                        Ok(reply) => reply,
                        Err(_) => continue,
                    };
                    assert!(reply.term >= current_term);
                    if reply.term > current_term {
                        let mut raft = raft.lock().unwrap();
                        assert!(raft.current_term >= current_term);
                        let role = Role::Candidate;
                        if raft.current_term > current_term || raft.role != role {
                            return;
                        }
                        raft.transfer(reply.term, Role::Follower);

                        return;
                    }
                    reply
                };
                if !reply.vote_granted {
                    continue;
                }
                votes += 1;
                if votes < majority {
                    continue;
                }

                let mut raft = raft.lock().unwrap();
                assert!(raft.current_term >= current_term);
                let role = Role::Candidate;
                if raft.current_term > current_term || raft.role != role {
                    return;
                }
                raft.transfer(current_term, Role::Leader);

                return;
            }
        })
    }
    fn spawn_leader_daemon(&self) {
        let total = self.peers.len();
        if total == 1 {
            return;
        }

        struct State {
            next_index:  Vec<u64>,
            match_index: Vec<u64>,
        }

        let state = Arc::new(Mutex::new(State {
            next_index:  vec![self.log.len(); total],
            match_index: vec![0; total],
        }));
        let major_index = (total + 1) / 2;
        self.other_peers()
            .for_each(|(id, other)| {
                let weak = Weak::clone(&self.weak);
                let cur_term = self.current_term;
                let state = Arc::clone(&state);

                let peer = other.clone();

                other.spawn(async move {
                    loop {
                        let Some(raft) = weak.upgrade()
                        else {
                            return;
                        };

                        enum Rpc {
                            AppendEntries(AppendEntriesArgs),
                            InstallSnapshot(InstallSnapshotArgs)
                        }
                        let (last_matched, rpc) = {
                            let raft = {
                                let raft = raft.lock().unwrap();
                                assert!(raft.current_term >= cur_term);
                                let role = Role::Leader;
                                // 新term,变成候选者，不是leader,就退出
                                if raft.current_term > cur_term
                                    || role == Role::Candidate && raft.role != role
                                {
                                    return;
                                }
                                raft
                            };
                            let next_index = state.lock().unwrap().next_index[id];

                            match next_index <= raft.log.offset {
                                true => {
                                    let args = InstallSnapshotArgs {
                                        term:                cur_term,
                                        last_included_term:  raft.log.first().term,
                                        last_included_index: raft.log.offset,
                                        data:                raft.persister.snapshot()
                                    };
                                    (raft.log.offset, Rpc::InstallSnapshot(args))
                                },
                                false => {
                                    let prev_log_index = next_index - 1;
                                    let args = AppendEntriesArgs {
                                        term: cur_term,
                                        prev_log_term: raft
                                            .log
                                            .get(prev_log_index)
                                            .unwrap()
                                            .term,
                                        prev_log_index,
                                        entries: raft
                                            .log
                                            .tail(next_index)
                                            .cloned()
                                            .collect(),
                                        leader_commit: raft.commit_index,
                                        leader_id: raft.me as u64
                                    };
                                    (raft.log.len() - 1, Rpc::AppendEntries(args))
                                }
                            }
                        };
                        if let Rpc::InstallSnapshot(args) = rpc {
                            _ = {
                                let mut rpc = peer.install_snapshot(&args).fuse();
                                let mut heart_beat_timer = Delay::new(HEARTBEAT_PERIOD).fuse();
                                let reply = futures::select! {
                                    reply = rpc => Some(reply),
                                    _ = heart_beat_timer => None,
                                };
                                if reply.is_none() {
                                    peer.spawn(async { _ = rpc.await });
                                    continue;
                                }
                                {
                                    let reply = match reply.unwrap() {
                                        Ok(reply) => reply,
                                        Err(_) => continue
                                    };
                                    assert!(reply.term >= cur_term);
                                    if reply.term > cur_term {
                                        let mut raft = raft.lock().unwrap();
                                        assert!(raft.current_term >= cur_term);
                                        let role = Role::Leader;
                                        if raft.current_term > cur_term ||  raft.role != role
                                        {
                                            return;
                                        }
                                        raft.transfer(reply.term, Role::Follower);
                                        return;
                                    }
                                    reply
                                }
                            };
                            {
                                let mut state = state.lock().unwrap();
                                state.next_index[id] = last_matched + 1;
                            }
                            Delay::new(HEARTBEAT_PERIOD).await;
                            continue;
                        }

                        let Rpc::AppendEntries(args) = rpc
                        else {
                            unreachable!()
                        };

                        let reply = {
                            let mut rpc = peer.append_entries(&args).fuse();
                            let mut heart_beat_timer = Delay::new(HEARTBEAT_PERIOD).fuse();
                            let reply = futures::select! {
                                reply = rpc => Some(reply),
                                _ = heart_beat_timer => None,
                            };
                            if reply.is_none() {
                                peer.spawn(async { _ = rpc.await });
                                continue;
                            }
                            {
                                let reply = match reply.unwrap() {
                                    Ok(reply) => reply,
                                    Err(_) => continue
                                };
                                assert!(reply.term >= cur_term);
                                if reply.term > cur_term {
                                    let mut raft = raft.lock().unwrap();
                                    assert!(raft.current_term >= cur_term);
                                    let role = Role::Leader;
                                    if raft.current_term > cur_term
                                        || role == Role::Candidate && raft.role != role
                                    {
                                        return;
                                    }
                                    raft.transfer(reply.term, Role::Follower);
                                    return;
                                }
                                reply
                            }
                        };
                        {
                            let mut raft = {
                                let raft = raft.lock().unwrap();
                                assert!(raft.current_term >= cur_term);
                                let role = Role::Leader;
                                if raft.current_term > cur_term
                                    || role == Role::Candidate && raft.role != role
                                {
                                    return;
                                }
                                raft
                            };
                            let mut state = state.lock().unwrap();

                            if !reply.success {
                                state.next_index[id] = raft
                                    .log
                                    .back(reply.conflicting_term)
                                    .unwrap_or_else(|| reply.first_conflicted.max(1))
                                    .min((state.next_index[id] - 1).max(1));
                            }
                            else if last_matched > state.match_index[id] {
                                state.next_index[id] = last_matched + 1;
                                state.match_index[id] = last_matched;
                                let mut match_index = state.match_index.clone();
                                match_index.sort_unstable();

                                let major_matched = match_index[major_index];
                                if matches!(raft.log.get(major_matched), Some(entry) if entry.term == cur_term) {
                                    raft.commit_and_apply(major_matched);
                                }
                            }
                            else {
                                state.next_index[id]=last_matched+1;
                            }
                        }
                        Delay::new(HEARTBEAT_PERIOD).await;
                    }
                })
            })
    }
    fn commit_and_apply(&mut self, to: u64) {
        assert!(to < self.log.len());
        if to > self.commit_index {
            self.commit_index = to
        }
        if to <= self.last_applied {
            return;
        }
        (self.last_applied + 1..=to).for_each(|i| {
            _ = self
                .apply_ch
                .unbounded_send(ApplyMsg::Command {
                    data: self
                        .log
                        .get(i)
                        .unwrap()
                        .data
                        .clone(),

                    index: i,
                });
        });

        self.last_applied = to;
    }
    fn other_peers(&self) -> impl Iterator<Item = (usize, &RaftClient)> {
        self.peers
            .iter()
            .enumerate()
            .filter(move |&(id, _)| id != self.me)
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        self.persister
            .save_raft_state(self.state());
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode::<Persistence>(data) {
            Ok(o) => {
                self.current_term = o.current_term;
                self.log = o.log.unwrap_or_default();
                self.voted_for = o.voted_for.map(|v| v as usize);
            },
            Err(e) => panic!("{:?}", e),
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        }
        else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, snapshot: Vec<u8>, term: u64, index: u64) {
        // Your code here (2D).
        assert!(index > self.commit_index);
        _ = self
            .apply_ch
            .unbounded_send(ApplyMsg::Snapshot { data: snapshot, term, index });

        self.commit_index = index;
        self.last_applied = index;
    }
    /// 更新term,转换身份
    fn transfer(&mut self, term: u64, role: Role) {
        assert!(term >= self.current_term);
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }

        self.role = role;
        match self.role {
            Role::Follower => self.spawn_follower_daemon(),
            Role::Candidate => self.spawn_candidate_daemon(),
            Role::Leader => self.spawn_leader_daemon(),
        }

        self.persist();
    }
    fn state(&self) -> Vec<u8> {
        let persistence = Persistence {
            current_term: self.current_term,
            voted_for:    self.voted_for.map(|id| id as u64),

            log: Some(self.log.clone()),
        };

        let mut state = Vec::new();
        labcodec::encode(&persistence, &mut state).unwrap();

        state
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        // self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
//
// 选择并发编程范式。
//
// 你可以通过rpc框架驱动Raft状态机，
//
// rust // struct Node { raft: Arc<Mutex<Raft>> } //
//
// 或者通过在一个新线程中运行Raft状态机并通过通道进行通信。
//
// rust // struct Node { sender: Sender<Msg> } //
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    pub fn size(&self) -> usize {
        self.raft
            .lock()
            .unwrap()
            .state()
            .len()
    }
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let raft = Arc::new(Mutex::new(raft));
        {
            let mut inner = raft.lock().unwrap();
            inner.weak = Arc::downgrade(&raft);
            inner.spawn_follower_daemon();
        } // 提前drop inner
        Self { raft }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    ///
    /// 使用Raft的服务（例如k/v服务器）希望就要追加到Raft日志中的下一个命令启动一致性协议。
    /// 如果该服务器不是领导者，将返回[Error::NotLeader]。否则，启动协议并立即返回。
    /// 不能保证该命令将被提交到Raft日志中，因为领导者可能会失败或失去选举。
    /// 即使Raft实例已被终止，此函数也应该正常返回。
    ///
    /// 元组的第一个值是命令如果被提交的话将出现的索引位置。第二个值是当前的任期。
    ///
    /// 此方法必须在不阻塞Raft的情况下返回。
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut raft = self.raft.lock().unwrap();
        if raft.role != Role::Leader {
            return Err(Error::NotLeader);
        }
        let mut data = vec![];
        labcodec::encode(command, &mut data).map_err(Error::Encode)?;

        let term = raft.current_term;
        raft.log
            .push(LogEntry::new(data, term));

        raft.persist();
        Ok((raft.log.len() - 1, term))
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft
            .lock()
            .unwrap()
            .current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term:      self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        let mut raft = self.raft.lock().unwrap();
        let a = raft.apply_ch.close();
        drop(a);
        raft.weak = Weak::new();
    }

    /// A service wants to switch to snapshot.
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        _last_included_term: u64,
        _last_included_index: u64,
        _snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        // crate::your_code_here((last_included_term, last_included_index, snapshot));
        true
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    ///
    /// 服务表示已创建一个快照，其中包含了截止到指定索引（包括该索引）的所有信息。
    /// 这意味着服务不再需要该索引及其之前的日志条目。Raft现在应该尽可能地修剪其日志。
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        let mut raft = self.raft.lock().unwrap();
        if index <= raft.log.offset {
            return;
        }
        raft.log.offset(index);
        raft.persister
            .save_state_and_snapshot(raft.state(), snapshot.into())
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    /// 向其他peer求票
    async fn request_vote(&self, args: RequestVoteArgs) -> RpcResult<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut raft = self.raft.lock().unwrap();
        {
            let reply = RequestVoteReply::new(raft.current_term, false);
            if args.term < raft.current_term {
                // println!("args term < curterm 拒绝投票");
                return Ok(reply);
            }
            if args.term > raft.current_term {
                raft.transfer(args.term, Role::Follower);
            }
        };

        let candidate_id = args.candidate_id as usize;
        assert!(candidate_id < raft.peers.len());

        let last_term_and_index = (raft.log.last().term, raft.log.len() - 1);
        if matches!(raft.voted_for,Some(id) if id != candidate_id)
            || (args.last_log_term, args.last_log_index) < last_term_and_index
        {
            return Ok(RequestVoteReply::new(raft.current_term, false));
        }

        raft.voted_for = Some(candidate_id);
        raft.last_sign = Instant::now();

        raft.persist();

        Ok(RequestVoteReply::new(raft.current_term, true))
    }
    /// 领导调用，复制日志
    async fn append_entries(&self, mut args: AppendEntriesArgs) -> RpcResult<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        {
            if args.term < raft.current_term {
                let reply = AppendEntriesReply::new(raft.current_term, false, 0, 0);
                return Ok(reply);
            }
            if args.term > raft.current_term {
                raft.transfer(args.term, Role::Follower);
            }

            assert!(raft.role != Role::Leader);
            match raft.role {
                // 收到心跳，重置起始时间
                Role::Follower => raft.last_sign = Instant::now(),
                _ => raft.transfer(args.term, Role::Follower),
            }
        };
        let len = raft.log.len();
        if args.prev_log_index >= len {
            return Ok(AppendEntriesReply::new(raft.current_term, false, 0, len));
        }
        let prev_entry = raft.log.get(args.prev_log_index);
        if matches!(prev_entry, Some(entry)if entry.term!=args.prev_log_term) {
            let term = prev_entry.unwrap().term;
            let last_without = (raft.log.offset..args.prev_log_index)
                .rev()
                .find(|&i| raft.log.get(i).unwrap().term != term)
                .unwrap_or(raft.log.offset);

            return Ok(AppendEntriesReply::new(
                raft.current_term,
                false,
                term,
                last_without + 1,
            ));
        }

        if prev_entry.is_none() {
            args.entries = args
                .entries
                .into_iter()
                .skip((raft.log.offset - args.prev_log_index) as usize)
                .collect();
            args.prev_log_term = raft.log.first().term;
            args.prev_log_index = raft.log.offset;
        }

        let log_offset = args.prev_log_index + 1;
        if let Some((i,_)) = args.entries.iter().enumerate().find(|&(i,entry)|{
            !matches!(raft.log.get(i as u64+log_offset),Some(last) if last.term==entry.term)
        }) {
            raft.log.truncate(i as u64+log_offset);
            raft.log.append(&mut args.entries.into_iter().skip(i).collect());
        }

        let last_index = raft.log.len() - 1;
        raft.commit_and_apply(args.leader_commit.min(last_index));

        raft.persist();
        Ok(AppendEntriesReply::new(raft.current_term, true, 0, 0))
    }
    async fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcResult<InstallSnapshotReply> {
        let mut raft = self.raft.lock().unwrap();
        {
            let term = args.term;
            {
                let reply = InstallSnapshotReply::new(raft.current_term);
                assert!(reply.term == raft.current_term);
                if term < raft.current_term {
                    return Ok(reply);
                }
                if term > raft.current_term {
                    raft.transfer(term, Role::Follower);
                }
            };
            assert!(raft.role != Role::Leader);
            if raft.role != Role::Follower {
                raft.transfer(term, Role::Follower);
            }
            else {
                raft.last_sign = Instant::now();
            }
        };

        if args.last_included_index <= raft.commit_index {
            return Ok(InstallSnapshotReply::new(raft.current_term));
        }
        assert!(
            args.last_included_term
                >= raft
                    .log
                    .get(raft.commit_index)
                    .unwrap()
                    .term
        );
        if matches!(
            raft.log.get(args.last_included_index),
            Some(entry) if entry.term == args.last_included_term
        ) {
            raft.log
                .offset(args.last_included_index);
        }
        else {
            raft.log
                .reset(args.last_included_term, args.last_included_index);
        }
        raft.snapshot(
            args.data.clone(),
            args.last_included_term,
            args.last_included_index,
        );
        raft.persister
            .save_state_and_snapshot(raft.state(), args.data);
        Ok(InstallSnapshotReply::new(raft.current_term))
    }
}
impl RequestVoteReply {
    fn new(term: u64, vote_granted: bool) -> Self {
        Self { term, vote_granted }
    }
}
impl InstallSnapshotReply {
    fn new(term: u64) -> Self {
        Self { term }
    }
}
impl AppendEntriesReply {
    fn new(term: u64, success: bool, conflicting_term: u64, first_conflicted: u64) -> Self {
        Self {
            term,
            success,
            conflicting_term,
            first_conflicted,
        }
    }
}
impl LogEntry {
    fn new(data: Vec<u8>, term: u64) -> Self {
        Self { data, term }
    }
}
