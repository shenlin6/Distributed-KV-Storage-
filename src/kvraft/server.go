package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

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

	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.deduplicateTable = make(map[int64]LastOperationInfo)

	// 从snapshot 中恢复数据
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				//如果消息处理过则忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
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

				// 判断是否需要 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid { //是快照的话需要恢复状态
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
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

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.deduplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

// restoreFromSnapshot 从快照中恢复（重启）
func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshot")
	}

	kv.stateMachine = &stateMachine
	kv.deduplicateTable = dupTable
}
