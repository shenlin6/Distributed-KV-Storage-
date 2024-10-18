package raft

import (
	"math/rand"
	"time"
)

// resetElectionLocked 重置选举时钟（加锁才能用）
func (rf *Raft) resetElectionLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeOutMax - electionTimeOutMin)
	rf.electionTimeOut = electionTimeOutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeOutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeOut
}

// isMoreUpdate 检查自己的最后一条日志和候选人的最后一条日志谁更新
func (rf *Raft) isMoreUpdateLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log,Me:[%d]T%d,Candidate:[%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term         int
	VotedGranted bool //投票是否通过
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VotedGranted = false
	//对齐 term
	if args.Term < rf.currentTerm {

		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,higher term,T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// 检查是否投过票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,Already voted to S%d", args.Term, rf.votedFor)
		return
	}

	// 检查当前 peer 的日志和候选者的日志哪一个更新
	if rf.isMoreUpdateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,Candidate less up-to-date", args.CandidateId)
		return
	}

	//满足所有条件才给票
	reply.VotedGranted = true
	rf.votedFor = args.Term
	rf.persistLocked()
	// 投票之后重置选举时钟
	rf.resetElectionLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Vote granted", args.CandidateId)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
// term. the third return value is true if this server believes it is
// the leader.

// startElection 发起选举，只对当前term负责
func (rf *Raft) startElection(term int) {
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{} //其他节点的响应
		ok := rf.sendRequestVote(peer, args, reply)

		//handle response
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "ask vote from S%d,Lost or error", peer)
			return
		}
		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		//check the context
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context,abort RequestVoteReply for S%d", peer)
			return
		}

		if reply.VotedGranted {
			votes++
			// 票数大于半数则变成Leader
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term) //发起心跳
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d],abort RequestVote", rf.role, term, rf.currentTerm)
		return
	}

	l := len(rf.log)

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}
		// 异步要票，否则阻塞主进程
		go askVoteFromPeer(peer, args)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//不是leader并且选举超时就发起选举
		if rf.role != Leader && rf.isElectionTimeOutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond) //sleep随机时间，最大程度避免peer同时发起选举/
	}
}
