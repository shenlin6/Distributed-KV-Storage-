package raft

import (
	"fmt"
	"sort"
	"time"
)

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

	// 用于更新 follower 的 commitIndex
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d,T%d,Prev: [%d]T%d,(%d,%d],CommitIdx: %d", args.LeaderId, args.Term, args.PrevLogIndex,
		args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//用于优化 Leader 的日志回溯
	ConfilictIndex int
	ConfilictTerm  int
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// AppendEntries peer 接受心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d,Appended,Args=%v", args.LeaderId, args.String())

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

	//重置时钟(由于可能匹配失败，一直没有重置时钟，可能导致时间过长，Follower 错误地发起选举)
	defer func() {
		rf.resetElectionLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// 如果 PrevLog 不匹配就返回错误
	if args.PrevLogIndex >= rf.log.size() { // 可能 peer 隔离太久了
		//日志过短，直接将 ConfilictIndex 设置为 follower 的最后一条日志,ConfilictTerm 置空
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Follower‘s log is too short,len %d<=Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	//日志不过短,Follower 存在 Leader.PrevLog,但不匹配,跳过对应 term的全部日志
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm { // 任期不同
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm) //任期不同直接跳过这个任期的所有 index,回退到上一个 term
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d reject log,Prev log not match,[%d]: T%d != T%d", args.LeaderId, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// 将 Leader 的日志目录添加到本地
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs:(%d, %d)", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 更新每个 peer 的 LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		// 唤醒 applicationTicker
		rf.applyCond.Signal()
	}
}

// sendAppendEntries 发送方发送心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// getMajorityIndexLocked 获取 peer 中 matchIndex 的众数
func (rf *Raft) getMajorityIndexLocked() int {
	tempIndexes := make([]int, len(rf.peers))
	copy(tempIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tempIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tempIndexes, majorityIdx, tempIndexes[majorityIdx])
	return tempIndexes[majorityIdx]
}

// startReplication Leader 对所有 peer 发送 RPC(只对参数中的 term 负责)
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d,Append, Reply=%v", peer, reply.String())

		//对齐 term(发现对方 term 更高就变成他的 Follower)
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 判断上下文是否丢失
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "->S:%d Context Lost T%d:Leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理 reply
		// 如果匹配不成功
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.Term == InvalidTerm { //说明日志太短了
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				// 以 Leader 日志为准，跳过 ConfilictTerm 的所有日志
				firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
				if firstTermIndex != InvalidIndex {
					rf.nextIndex[peer] = firstTermIndex + 1
				} else { //发现 Leader 日志中不存在 ConfilictTerm 的任何日志，则以 Follower 为准跳过 ConflictTerm,使用 ConfilictIndex。
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// 避免乱序的 reply 影响匹配效率
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S:%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer,
				args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log.at(rf.nextIndex[peer]-1).Term)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.log.String())
			return
		}

		// 匹配成功，更新matchIndex
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 匹配点只对当前的参数负责，因为可能发送 RPC 同时会有新的 log 进来
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 更新 commitIndex,进而下发给 follower，指导 follower本地的 reply
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm { //这里不能提交 peer 之前的日志,需要压一到
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			// 唤醒 applicationTicker
			rf.applyCond.Signal()
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
			// Leader 自己给自己更新 matchIndex
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log.at(prevIdx).Term

		//如果视图匹配上了就发送 Leader 的 prevIdx 后面所有的日志
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> Append, Args=%v", peer, args.String())
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
