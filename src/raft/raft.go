package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"github.com/aganhui/go_common/logger"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//ygh: 2A
	timeoutkey string

	Term     int
	VoteID   int //ygh: 是否有必要设置此变量
	LeaderID int
	status   string

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var Term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	Term = rf.Term
	isleader = rf.status == StrStatusLeader
	rf.mu.Unlock()
	return Term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term   int
	PeerID int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term     int
	VoteID   int
	LeaderID int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	str := rf.get4items()
	logger.Debugf("server %s get voterequest: (%d, %d)\n", str, args.Term, args.PeerID)
	rf.mu.Lock()
	if args.Term < rf.Term {
		reply.Term = rf.Term
		reply.VoteID = NullPeerID
		reply.LeaderID = rf.LeaderID //ygh: 1.Null 2.true LeaderID
		rf.mu.Unlock()
		return
	} else if args.Term == rf.Term {
		if rf.VoteID == NullPeerID {
			str := "vote by VoteRequest"
			rf.update4items(str, rf.Term, args.PeerID, NullPeerID, StrStatusFollower)
			go rf.ticker()

			reply.Term = args.Term
			reply.VoteID = args.PeerID
			reply.LeaderID = NullPeerID
			rf.mu.Unlock()
			return
		} else { //ygh: 已投票给其它节点
			reply.Term = args.Term
			reply.VoteID = NullPeerID
			reply.LeaderID = rf.LeaderID
			rf.mu.Unlock()
			return
		}
	} else { //ygh: args.Term > rf.Term
		str := "vote and update by VoteRequest"
		rf.update4items(str, args.Term, args.PeerID, NullPeerID, StrStatusFollower)
		go rf.ticker()

		reply.Term = args.Term
		reply.VoteID = args.PeerID
		reply.LeaderID = NullPeerID
		rf.mu.Unlock()
		return
	}
	logger.Debugf("err execution!\n")
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, baseterm int, votech chan int, returnch chan int) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		returnch <- ReturnFailed //ygh: 此选民连接失败
		return
	}

	rf.mu.Lock()
	if rf.Term > baseterm { //ygh: 有其它的协程导致rf.Term增加
		rf.mu.Unlock()
		returnch <- ReturnBreak //ygh: 结束所有vote
		return
	} //ygh: 其余情况都是rf.Term == args.Term
	if reply.LeaderID != NullPeerID { //ygh: 当前节点直接确定leader
		str := "candidate to follower by VoteReply"
		rf.update4items(str, reply.Term, NullPeerID, reply.LeaderID, StrStatusFollower)
		go rf.ticker()
		rf.mu.Unlock()
		returnch <- ReturnBreak //ygh: 结束所有vote
		return
	}
	if reply.Term == args.Term {
		if reply.VoteID == args.PeerID {
			votech <- 1
		}
	} else { //ygh: reply.Term > args.Term
		str := "candidate to follower by VoteReply"
		rf.update4items(str, reply.Term, NullPeerID, NullPeerID, StrStatusFollower)
		go rf.ticker()
		rf.mu.Unlock()
		returnch <- ReturnBreak //ygh: 结束所有vote
		return
	}
	rf.mu.Unlock()

	returnch <- ReturnSucceed //ygh: 顺利结束
	return
}

func (rf *Raft) collectVotes(baseterm int, votenum int) {
	logger.Debugf("server %d start to collectVotes(term is %d)\n", rf.me, baseterm)
	peernum := len(rf.peers) //ygh: 若peer数量在变化呢？
	returnch := make(chan int, peernum)
	votech := make(chan int, peernum)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		rf.mu.Lock()
		if rf.Term != baseterm { //ygh: 在锁内作判断以使得取到的Term与初始的一样
			rf.mu.Unlock()
			return
		}
		args := &RequestVoteArgs{
			Term:   rf.Term,
			PeerID: rf.me,
		}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		logger.Debugf("send request: %d to %d\n", rf.me, idx)
		go rf.sendRequestVote(idx, args, reply, baseterm, votech, returnch)
	}
	returnnum := 0
	for {
		select {
		case returnflag := <-returnch:
			// logger.Debugf("return!!!!!\n")
			if returnflag == ReturnBreak {
				return
			} else { //ygh: returnflag: ReturnFailed or ReturnSucceed
				returnnum += 1
				logger.Debugf("server %d returnnum is %d\n", rf.me, returnnum)
				if returnnum == peernum-1 {
					logger.Debugf("server %d all return!!!!!\n", rf.me)
					goto COLLECTVOTE
				}
			}
		case <-votech:
			// logger.Debugf("vote!!!!!\n")
			votenum += 1
		case <-time.After(TimeoutForCollectVote):
			logger.Debugf("server %d collect vote timeout\n", rf.me)
			goto COLLECTVOTE
		}
	}

COLLECTVOTE:
	logger.Debugf("server %d collect vote!!!!!\n", rf.me)
	rf.mu.Lock()
	if rf.Term > baseterm { //ygh: 有其它的协程导致rf.Term增加
		rf.mu.Unlock()
		return
	}
	logger.Debugf("%d get votes num %d\n", rf.me, votenum)
	if votenum >= int(math.Floor(float64(len(rf.peers))/2))+1 {
		//ygh: todo, i am the new leader
		str := "become leader"
		rf.update4items(str, rf.Term, rf.VoteID, rf.me, StrStatusLeader)
		go rf.runHeartbeat(rf.Term)
	}
	rf.mu.Unlock()
	return
}

type HeartbeatArgs struct {
	Term     int
	LeaderID int
}
type HeartbeatReply struct {
	Term     int
	LeaderID int
}

func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	if rf.Term <= args.Term {
		str := "update term by Heartbeat"
		rf.update4items(str, args.Term, EmptyInt, args.LeaderID, StrStatusFollower)
		go rf.ticker()
		reply.Term = args.Term
		reply.LeaderID = args.LeaderID
	} else {
		reply.Term = rf.Term
		reply.LeaderID = rf.LeaderID
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendHeartbeat(server int, baseterm int, args *HeartbeatArgs, reply *HeartbeatReply, returnch chan int) {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	if !ok {
		returnch <- ReturnFailed
		return
	}

	rf.mu.Lock()
	if rf.Term != baseterm || rf.status != StrStatusLeader || rf.LeaderID != rf.me {
		rf.mu.Unlock()
		returnch <- ReturnBreak
		return
	}
	if reply.Term > baseterm { //ygh: leader直接变成follower
		str := "leader to follower by Heartbeat"
		rf.update4items(str, reply.Term, NullPeerID, reply.LeaderID, StrStatusFollower)
		go rf.ticker()
		rf.mu.Unlock()
		returnch <- ReturnBreak
		return
	}
	rf.mu.Unlock()

	returnch <- ReturnSucceed
	return
}

func (rf *Raft) runHeartbeat(baseterm int) {
	peernum := len(rf.peers)
	for rf.Term == baseterm && rf.status == StrStatusLeader && rf.LeaderID == rf.me {
		returnch := make(chan int, peernum)
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}

			rf.mu.Lock()
			if rf.Term != baseterm || rf.status != StrStatusLeader || rf.LeaderID != rf.me {
				rf.mu.Unlock()
				return
			}
			args := &HeartbeatArgs{
				Term:     rf.Term,
				LeaderID: rf.me,
			}
			rf.mu.Unlock()
			reply := &HeartbeatReply{}
			go rf.sendHeartbeat(idx, baseterm, args, reply, returnch)
		}
		go rf.ticker() //ygh: 更新状态，防止timeout
		time.Sleep(LeaderHeartbeatInterval)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true

	// Your code here (2B).

	return index, Term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	if rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		localkey := GetUid()
		rf.timeoutkey = localkey
		rf.mu.Unlock()

		time.Sleep(GetRandomTimeoutDuration())

		rf.mu.Lock()
		if rf.timeoutkey != localkey {
			rf.mu.Unlock()
			return
		}
		//ygh: start new Term
		str := "ticker timeout"
		rf.update4items(str, rf.Term+1, rf.me, NullPeerID, StrStatusCandidate)
		go rf.ticker()
		localterm := rf.Term
		rf.mu.Unlock()
		//ygh: todo, request for vote
		rf.collectVotes(localterm, 1)
	}
}

func (rf *Raft) update4items(info string, term int, voteid int, leaderid int, status string) {
	if term == EmptyInt {
		term = rf.Term
	}
	if voteid == EmptyInt {
		voteid = rf.VoteID
	}
	if leaderid == EmptyInt {
		leaderid = rf.LeaderID
	}
	if status == EmptyStr {
		status = rf.status
	}
	str := rf.get4items()
	logger.Debugf("server: %s to (%d, %d, %d, %s), %s\n", str, term, voteid, leaderid, status, info)
	rf.Term = term
	rf.VoteID = voteid
	rf.LeaderID = leaderid
	rf.status = status
	return
}

func (rf *Raft) get4items() string {
	return fmt.Sprintf("%d (%d, %d, %d, %s)", rf.me, rf.Term, rf.VoteID, rf.LeaderID, rf.status)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Term = 1
	rf.VoteID = NullPeerID
	rf.LeaderID = NullPeerID
	rf.status = StrStatusFollower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
