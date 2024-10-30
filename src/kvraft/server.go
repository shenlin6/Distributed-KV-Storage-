package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied      int
	stateMachine     *MemoryKVStateMachine
	notifyChans      map[int]chan *OpReply
	deduplicateTable map[int64]LastOperationInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 将操作存储在 raft 日志中并同步给follower
	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: OpGet,
	})

	// 不是 Leader 的话返回让客户端重试
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err

	case <-time.After(ClientRequestTimeOut):
		reply.Err = ErrTimeout
	}

	// 异步回收 channel
	go func() {
		kv.mu.Lock()
		kv.recycleNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	//判断客户端的请求是否重复
	kv.mu.Lock()
	if kv.isDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复的请求，直接返回结果
		OpReply := kv.deduplicateTable[args.ClientId].Reply
		reply.Err = OpReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 将操作存储在 raft 日志中并同步给follower
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	// 不是 Leader 的话返回让客户端重试
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeOut):
		reply.Err = ErrTimeout
	}

	// 异步回收 channel
	go func() {
		kv.mu.Lock()
		kv.recycleNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicated(clientId, seqId int64) bool {
	info, ok := kv.deduplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.deduplicateTable = make(map[int64]LastOperationInfo)

	go kv.applyTask()
	return kv
}

func (kv *KVServer) applyTask() {
	if !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()

				//如果消息处理过则忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					// 这里可能有 bug TODO
				}
				kv.lastApplied = message.CommandIndex
				// 取出用户的操作信息
				op := message.Command.(Op)

				//去重（重复的客户端 putappend 请求）
				var opReply *OpReply
				if op.OpType != OpGet && kv.isDuplicated(op.ClientId, op.SeqId) {
					opReply = kv.deduplicateTable[op.ClientId].Reply
				} else {
					//应用到状态机中
					opReply = kv.applyToStateMachine(op)
					//保存一下已经应用到状态机中的请求
					if op.OpType != OpGet {
						kv.deduplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将结果返回回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChan(message.CommandIndex)
					notifyCh <- opReply
				}

				kv.mu.Unlock()
			}
		}
	}
}

// 应用到状态机中
func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err

	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}

	return &OpReply{Value: value, Err: err}
}

func (kv *KVServer) getNotifyChan(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) recycleNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}
