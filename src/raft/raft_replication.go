package raft

import "time"

type LogEntry struct {
	Term         int         // the log entry's term
	CommandValid bool        // is it should be applied
	Command      interface{} //	the command should be applied to the state machine
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match point
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries peer 接受心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	//对齐 term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Higher term,T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 如果 PrevLog 不匹配就返回错误
	if args.PrevLogIndex > len(rf.log) { // 可能 peer 隔离太久了
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Follower‘s log is too short,len %d<=Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // 任期不同
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d reject log,Prev log not match,[%d]: T%d != T%d", args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 将 Leader 的日志目录添加到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs:(%d, %d)", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// TODO: 更新每个 peer 的 LeaderCommit

	//重置时钟
	rf.resetElectionLocked()
}

// sendAppendEntries 发送方发送心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// startReplication 对所有 peer 发送 RPC
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "->S%d Lost or crashed", peer)
			return
		}

		//对齐 term(发现对方 term 更高就变成他的 Follower)
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

	}

	// rf.currentTerm 可能被并发修改，需要加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果上下文丢失那么则不再发送心跳
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// replicationTicker 心跳（日志同步）：只能在传入的 term 内才能进行日志同步
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		// sleep 一个比 electionTimeOutMin 更小的时间，防止有异心的 peer 发起选举
		time.Sleep(replicateInterval)
	}
}
