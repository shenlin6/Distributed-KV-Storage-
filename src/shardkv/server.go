package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead             int32
	lastApplied      int
	stateMachine     *MemoryKVStateMachine
	notifyChans      map[int]chan *OpReply
	deduplicateTable map[int64]LastOperationInfo
	currentConfig    shardctrler.Config
	mck              *shardctrler.Clerk
}

func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	return kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) isDuplicated(clientId, seqId int64) bool {
	info, ok := kv.deduplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//判断请求的 Key 是否属于当前的 group
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//判断请求的 Key 是否属于当前的 group
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	//判断客户端的请求是否重复
	if kv.isDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复的请求，直接返回结果
		OpReply := kv.deduplicateTable[args.ClientId].Reply
		reply.Err = OpReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 将操作存储在 raft 日志中并同步给follower
	index, _, isLeader := kv.rf.Start(
		Op{
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

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.deduplicateTable = make(map[int64]LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()

	// 从snapshot 中恢复数据
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	go kv.getConfigTask()
	return kv
}

// 应用到状态机中
func (kv *ShardKV) applyToStateMachine(op Op) *OpReply {
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

func (kv *ShardKV) getNotifyChan(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) recycleNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.deduplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

// restoreFromSnapshot 从快照中恢复（重启）
func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
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
