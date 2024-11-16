package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead             int32 // set by Kill()
	lastApplied      int
	stateMachine     *CtrlerStateMachine
	notifyChans      map[int]chan *OpReply
	deduplicateTable map[int64]LastOperationInfo
}

func (sc *ShardCtrler) isDuplicated(clientId, seqId int64) bool {
	info, ok := sc.deduplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	var opReply OpReply
	sc.command(Op{
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	var opReply OpReply
	sc.command(Op{
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	var opReply OpReply
	sc.command(Op{
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	var opReply OpReply
	sc.command(Op{
		OpType: OpQuery,
		Num:    args.Num,
	}, &opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err

}

func (sc *ShardCtrler) command(args Op, reply *OpReply) {
	//判断客户端的请求是否重复
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.isDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复的请求，直接返回结果
		OpReply := sc.deduplicateTable[args.ClientId].Reply
		reply.Err = OpReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	sc.mu.Lock()
	notifyCh := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-notifyCh:
		reply.ControllerConfig = result.ControllerConfig
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeOut):
		reply.Err = ErrTimeout
	}

	// 异步回收 channel
	go func() {
		sc.mu.Lock()
		sc.recycleNotifyChannel(index)
		sc.mu.Unlock()
	}()
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.deduplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()
	return sc
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				//如果消息处理过则忽略
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex
				// 取出用户的操作信息
				op := message.Command.(Op)

				//去重（重复的客户端 putappend 请求）
				var opReply *OpReply
				if op.OpType != OpQuery && sc.isDuplicated(op.ClientId, op.SeqId) {
					opReply = sc.deduplicateTable[op.ClientId].Reply
				} else {
					//应用到状态机中
					opReply = sc.applyToStateMachine(op)
					//保存一下已经应用到状态机中的请求
					if op.OpType != OpQuery {
						sc.deduplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将结果返回回去
				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyCh := sc.getNotifyChan(message.CommandIndex)
					notifyCh <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

// 应用到状态机中
func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	var err Err
	var cfg Config
	switch op.OpType {
	case OpQuery:
		cfg, err = sc.stateMachine.Query(op.Num)
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	}
	return &OpReply{ControllerConfig: cfg, Err: err}

	return nil
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) recycleNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}
