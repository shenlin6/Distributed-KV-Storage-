package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const (
	electionTimeOutMin time.Duration = 250 * time.Millisecond
	electionTimeOutMax time.Duration = 400 * time.Millisecond
	replicateInterval  time.Duration = 70 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0 //表示没有找到对应 log 的 index
)

type Role string

const (
	Follower  Role = "Follwer"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 快照，日志压缩
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int
	votedFor    int //-1 means vote for none

	// 每个 peer 本地的日志
	log *RaftLog

	// Leader使用的,相当于每个 peer 的视图
	nextIndex  []int
	matchIndex []int

	// 用于 apply 的字段
	lastApplied int
	commitIndex int
	applych     chan ApplyMsg
	snapPending bool
	applyCond   *sync.Cond //日志 apply 只在被唤醒时触发

	electionStart   time.Time
	electionTimeOut time.Duration // Random
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower,lower term:T%d", term)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower,For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	shouldPersist := rf.currentTerm != term //看是否需要持久化
	//投票置空
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	//
	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become a candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate,For T%d", rf.role, rf.currentTerm+1) //提前打日志，防止覆盖
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become a Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become a Leader in T%d", rf.currentTerm)
	rf.role = Leader

	//初始化 Leader 对于 peers 的视图
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start 将日志发送给 Leader,
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//保证系统只有一个对外数据接收点
	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		Term:         rf.currentTerm,
		CommandValid: true,
		Command:      command,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
}

// Kill
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

// contextLostLocked 检测是否仍为发送 RPC 之前的角色以及 term (收到 RPC 返回值时不知道过了多少时间)
// 需要及时终止回调函数、请求返回的处理，否则状态机会产生问题
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

// Make
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

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.log = NewRaftLog(InvalidIndex, InvalidTerm, nil, nil) //初始化：首先加入一个空的日志，可以避免很多边界判断

	// 初始化 Leader 对于 peer 的视图
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// 初始化用于 apply 的字段
	rf.applych = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapPending = false

	// initialize from state persisted before a crash(注意在赋值之后再调用)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
