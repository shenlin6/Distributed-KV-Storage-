# Raft 部分

## PartA 领导者选举

领导者选举的核心在于比较节点之间日志的新旧，而日志的新旧取决于节点当前的**任期**和**日志的序号**。

**任期**是决定优先级的第一因素。低任期的 Peer 收到高任期的 Peer 任何信息后，会自动“跟上”（Follow）任期变成**跟随者**（Follower）。高任期的 Peer 收到低任期 Peer 的任何**请求**时，会直接拒绝。**任期的增加触发于：Follower参加选举变为 Candidate**，这时候该peer 的任期会+1。

在 Raft 中，如果出现**网络分区**，某些 Peer 被隔绝，也很容易不知道其他 Peer 到了哪个 Term。如果被隔离的 Peer 重新和其他节点建立起了连接，首先就需要**对齐任期**。

在**选举投票**的时候，每个 candidate 都要比较谁的日志更新更全，一旦 follower 投票给了某个 candidate，就要重置选举时钟，保证自己在一段时间内不参与选举。

关于两个 Peer 所存日志谁更新的问题，**Term越高日志越新，Term相同，日志的index越大越新。**

投票处理的逻辑如下：

1. 如果说 candidate 的任期比自己大，那么投票
2. 如果 candidate  的扔弃和自己一样，但是日志的 index 大于自己，投票

```go
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
```

注意：

1. 每次 RPC 通信的时候可能因为网络原因导致耗时不稳定，因此需要异步地开启选举。

2. 每次发起选举的的间隔时间都要随机，避免多个节点同时选举，导致几个选举周期下来还没选出 leader

   

核心的选举逻辑：

```go
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
       LOG(rf.me, rf.currentTerm, DDebug, "-> S%d,AskVote Reply=%v", peer, reply.String())

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

    lastIdx, lastTerm := rf.log.last()
    for peer := 0; peer < len(rf.peers); peer++ {
       if peer == rf.me {
          votes++
          continue
       }
       args := &RequestVoteArgs{
          Term:         rf.currentTerm,
          CandidateId:  rf.me,
          LastLogIndex: lastIdx,
          LastLogTerm:  lastTerm,
       }
       LOG(rf.me, rf.currentTerm, DDebug, "-> S%d AskVote, Args=%v", peer, args.String())

       // 异步要票，否则阻塞主进程
       go askVoteFromPeer(peer, args)
    }
}
```

Leader当选后，会对所有其他人进行**心跳压制**，只要其他 follower 的任期没有 leader大，就必须重置选举时钟。这里需要注意选举超时和心跳压制的时间关系，至少选举超时时间不能大于心跳压制的时间，否则已经有 Leader 当选，但还是不断的有 Peer 发起选举。

心跳压制逻辑：

```go
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
```

## PartB 日志同步

日志同步的主要目标是为了**确保所有peer有相同的日志记录。**

因此Leader需要维护一个各个 Peer 的进度视图，即`nextIndex` 和 `matchIndex` 数组。其中：`nextIndex` 用于进行**日志同步时**的**匹配点试探**，`matchIndex` 用于**日志同步成功后**的**匹配点记录**。据全局匹配点分布，我们可以计算出当前全局的 `commitIndex`，然后再通过之后轮次的日志复制 RPC 下发给各个 Follower。每个 Follower 收到 `commitIndex` 之后，再去 apply 本地的已**提交日志到状态机**。

具体：**在心跳压制的时候，也需要同时进行日志同步**，最简单粗暴的方法就是，每次 leader要进行日志同步的时候，leader都把自己的的全量日志发送给 follower，但是 leader的日志量比较大的时候，会造成比较多的资源浪费，因为日志不同步的情况并不是频繁发生。

可以采取**乐观+回撤**的算法进行**日志匹配点试探**。所谓日志匹配：就是**相同 Index 的地方，Term 相同**；即 **index 和 term 能唯一确定一条日志**，这是因为，Raft 算法保证一个 Term 中最多有（也可能没有）一个 Leader，然后只有该 Leader 能确定日志顺序且同步日志。这样一来，Term 单调递增，每个 Term 只有一个 Leader，则该 Leader 能唯一确定该 Term 内的日志顺序。

第一次向 follower发送心跳压制的时候并不发送日志，而是只发送自己的任期和最新日志的 index（任**期和该任期的index可以唯一确定一条日志，因为raft集群中一个term只有一个leader**），如果说 follower 都对得上号，那么后面的心跳也不需要带上任何日志了。如果说 follower 发现自己的日志与 leader对不上号，那么下次 leader发送心跳压制的同时，向前回撤一条日志，如此循环，**直到两边日志匹配上了**（至少在第零条日志对上号），然后将从回撤到的这条日志的位置开始，到leader 最新的日志，同步到follower中，完成日志同步。

注意：如果 follower的任期小于 leader的任期，我们一条一条日志甚至于一个一个任期地进行回撤，那么每次回撤后都需要下一个 RPC 才能发给 Follower，这样**可能导致匹配探测期时间特别长**。

优化：新增两个字段：ConfilictIndex 和 ConfilictTerm

对于 Follower:

1. 如果说Follower 日志过短，甚至**短于 Leader要求匹配的前一个日志条目索引**（PrevLogIndex），说明可能这个节点被隔离时间太久了，可以让 Leader 迅速回退到 Follower 日志的末尾，而不用一个个 index 或者 term 往前试探。
2. 如果说如果 Follower 存在 `Leader.PrevLog` ，**但 term 不匹配**，则让 Leader 将对应冲突的日志直接全部跳过。然后从Leader.PrevLog 所在任期地最后一条日志开始往前回撤。

对于 Leader：

1. 如果 `ConfilictTerm` 为空，说明 Follower 日志太短，直接将 `nextIndex` 赋值为 `ConfilictIndex` 迅速回退到 Follower 日志末尾。
2. 否则，**以 Leader 日志为准**，跳过 `ConfilictTerm` 的所有日志；如果发现 Leader 日志中不存在 `ConfilictTerm` 的任何日志，则**以 Follower 为准**跳过 `ConflictTerm`，即使用 `ConfilictIndex`。

这种优化在以下情况特别有效：

Follower 刚刚加入集群

Follower 经历了长时间的网络分区

Follower 的日志严重落后于 Leader

优化代码如下：

```go
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
```



核心的日志同步逻辑：

```go
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
          if reply.ConfilictTerm == InvalidTerm { //说明日志太短了
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

          nextPrevIndex := rf.nextIndex[peer] - 1
          nextPrevTerm := InvalidTerm
          if nextPrevIndex >= rf.log.snapLastIdx {
             nextPrevTerm = rf.log.at(nextPrevIndex).Term
          }
          LOG(rf.me, rf.currentTerm, DLog, "-> S:%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer,
             args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, nextPrevTerm)
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
       // 发现日志被截断了
       if prevIdx < rf.log.snapLastIdx {
          args := &InstallSnapshotArgs{
             Term:              rf.currentTerm,
             LeaderId:          rf.me,
             LastIncludedIndex: rf.log.snapLastIdx,
             LastIncludedTerm:  rf.log.snapLastTerm,
             Snapshot:          rf.log.snapshot,
          }
          LOG(rf.me, rf.currentTerm, DDebug, "-> S%d SendSnap, Args=%v", peer, args.String())
          go rf.installToPeer(peer, term, args)
          continue
       }

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
       LOG(rf.me, rf.currentTerm, DDebug, "->S%d, Append, Args=%v", peer, args.String())
       go replicateToPeer(peer, args)
    }
    return true
}
```

这部分的最终目的，就是要更新 `matchIndex`。Leader 有了 `commitIndex` 之后，据此全局匹配点视图，我们可以**算出多数 Peer 的匹配点**，进而更新 Leader 的 `CommitIndex`。再将其下发给各个 Follower，**指导其各自更新本地 `commitIndex` 进而 apply**。

```go
// getMajorityIndexLocked 获取 peer 中 matchIndex 的众数
func (rf *Raft) getMajorityIndexLocked() int {
	tempIndexes := make([]int, len(rf.peers))
	copy(tempIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tempIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tempIndexes, majorityIdx, tempIndexes[majorityIdx])
	return tempIndexes[majorityIdx]
}
```

## PartC 状态持久化

为了让某个 Peer 异常重启后，能够正常重新加入集群的，需要将 Raft 的关键信息定时持久化，**发生异常后重启后加载进来**。这里直接使用测试框架中的 **labgob** 包来进行序列化和反序列化。

论文中提高了三个需要持久化的字段：`currentTerm`，`votedFor` 和 `log`。需要持久化的原因如下

对于currentTerm：在异常重启后一定要知道自己重启之前的任期是多少，因为任期是准则。

对于votedFor：如果已经投过票了，之后就不能再投票，因此要记录是否投票这个状态

对于log：日志不持久化就丢了，哈哈

具体实现如下：

```go
//persistLocked 序列化
func (*rf* *Raft) persistLocked() {
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  rf.log.persist(e)
  raftstate := w.Bytes()
  rf.persister.Save(raftstate, rf.log.snapshot)
  LOG(rf.me, rf.currentTerm, DPersist, "Persist:,%v", rf.persistString())
}

// readPersist 反序列化
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("Decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("Decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("Decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

```

注意：重启调用 `readPersist` 的时候**需要只有在构造Raft 实例时才调用**，如果是先调用 `readPersist` 再去初始化，就会把 `readPersist` 读出来的值给覆盖掉。

## PartD 日志压缩

对于长时间运行的 Raft 集群，如果持续收到日志，将日志无限追加并且没有压缩的话，空间会膨胀得很快，并且重启时重放所有日志会变得很慢，因此需要定时**对日志做快照**，对某个日志以及之前的日志**做了快照之后**，**这个日志以及之前的日志会被截断。**

实现快照的流程大致如下：

1. Leader应用层保存 snapshot，截断对应日志，然后持久化
2. Leader 在下次 RPC 中将 snapshot 相关参数传递给 Follower（snapLastTerm、snapLastIndex）
3. Follower 接收到 Leader RPC 后替换本地日志，并将其持久化

压缩日志的逻辑如下：

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		return
	}
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}
```

注意：

1. 如果尝试的快照索引大于当前已提交的索引，则直接返回

2. 如果要创建快照的位置已经被包含在之前的快照中，则不需要重复创建

   

但是如果 Leader 想给从匹配节点发送日志时，要先检查要发送的 PrevLogEntry 还在不在，**发现相关日志条目已经被截断**，这个时候不能直接发送日志，**需要先将 Leader 的 Snapshot 直接同步给 Follower**，先让这个落后太多的节点快速追赶，再做之后日志同步。

具体逻辑如下：

```go
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

	// 将快照持久化
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	//新建函数，使 GC 能够回收
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}
```

日志压缩的标志就是如果说应用层调用了 `snapshot(index, snapshot)`，就意味着 Raft 层可以把 index 及以前的日志给释放掉了，Raft 层要保存下这个 Snapshot（在内存中保存和持久化到外存）

## 数据分片

在不分区的情况下，所有数据的读写请求都会在一个分片中，这在并发量较大的情况下可能存在一定的瓶颈。如果对数据做了**分区**，那么不同分区之间的数据读写请求是可以并行的，这能够较大的提升 KV 系统的并发能力。

数据分片主要有两部分组成：复制组（Replica Group)和分片控制器（shard controller）

第一个部分：复制组是指处理一个或多个**分片**（shard）组成的KV 服务，是由一个 Raft 集群组成的。每个复制组为一个分区，**每个分区负责一部分分片的读写请求和数据存储**。

第二个部分：分片控制器，它主要是存储系统元数据，一般是一些配置信息，例如每个 Group 应该负责哪些 shard，这个配置信息是有可能发生变化的。客户端首先会从 shard controller 获取请求 key 所属的 Group，并且 Group 也会从 shard controller 中获取它应该负责哪些 shard。

**数据分片的核心在于处理配置的变更**， 即 shard 到 Group 的映射关系。解决方案：1. 将配置变更的请求也传到 raft 模块中进行状态同步，需要保证一个 shard 在同一时刻，只能被一个 Group 所负责。2. 配置变更也需要各个 Group 之间进行数据传输，如果一个 shard 在配置变更期间的所有权转移到了另一个 Group 中，那么一个 Group 就需要从原来的 Group 中获取这个 shard 的所有数据。

shard controller 的实现：主要提供四个方法用于配置变更：**Query、Move、Join、Leave**

`Query` 方法查询并返回指定编号的配置信息。

`Move` 方法的参数是一个 shard 编号和一个 Group ID。主要是用于将一个 shard 移动到指定的 Group 中。

`Join` 方法是添加新的 Group，通过唯一标识 GID 到服务节点名字列表的映射关系。**需要处理添加完成之后的负载均衡问题。**可以从拥有最多 shard 的 Group 中取出一个 shard，将其分配给最少 shard 的那个 Group，如果最多和最少的 shard 的差值小于等于 1，那么说明就已经达到了平衡，否则的话就按照同样的方法一直重复移动 shard。

`Leave` 方法的参数是一组集群中的 Group ID，表示这些 Group 退出了分布式集群。在删除掉集群中的 Group 之后，其负责的 shard 应该转移到其他的 Group 中，重新让集群达到均衡。

注意：

因为分片在迁移过程中不能同时处理新请求，否则可能出现数据不一致的问题。比如：

Group1 开始迁移分片S，数据为 {key1:A}
时间点2: 客户端向Group1写入 key1:B
时间点3: Group1完成迁移，Group2收到的是 {key1:A}
时间点4: 客户端向Group2读取 key1，得到的是A而不是B

因此在配置变更期间，在一些 Group 之间可能需要双向传递 shard，可能发生了**死锁**：

假设有两个 Group：G1 和 G2

- G1 需要将分片 S1 转移给 G2

- G2 需要将分片 S2 转移给 G1

- 双方都在等待对方完成迁移

解决方法：

1. **设置版本号来决定迁移顺序**，版本号大的配置优先处理。
2. 单向迁移原则：设置 Normal、Pulling（拉取中）、GC（清理中）三种状态，保证只有处于Normal状态的两个分区才可以进行单向的分片迁移操作

当需要交换分片时：

1. 配置号更大的迁移先执行完成
2. 配置号小的迁移后执行
3. 每次迁移都是完整的"拉取-清理"流程

例如：Group1 和 Group2 需要交换分片 S1、S2：

步骤1: Group2(**新配置号更大**)先完成 S1 的迁移

- Group2 拉取 S1
- Group1 响应请求
- Group1 等待清理通知
- Group2 发送清理通知
- Group1 清理完成

步骤2: Group1 再完成 S2 的迁移

- Group1 拉取 S2
- Group2 响应请求
- Group2 等待清理通知
- Group1 发送清理通知
- Group2 清理完成
