package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term     int
	LeaderId int //投票是否通过

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d,ResvSnapShot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	// 对齐 term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap,Higher Term: T%d-T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	//看本地是否已经包含这个 snapshot
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap,Already installed: %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

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

	// 更新 match index 和 next index
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}

}
