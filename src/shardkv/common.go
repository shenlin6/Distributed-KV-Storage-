package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrWrongConfig    = "ErrWrongConfig"
	ErrConfigNotReady = "ErrConfigNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	ClientRequestTimeOut = 500 * time.Millisecond
	GetConfigInterval    = 100 * time.Millisecond
	ShardMigration       = 50 * time.Millisecond
	ShardGcInterval      = 50 * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Value string
	Err   Err
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

func getOperationType(s string) OperationType {
	switch s {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type %s", s))
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

// 拷贝去重表
func (l *LastOperationInfo) copyData() LastOperationInfo {
	return LastOperationInfo{
		SeqId: l.SeqId,
		Reply: &OpReply{
			Err:   l.Reply.Err,
			Value: l.Reply.Value,
		},
	}
}

// 传入 Raft
type RaftCommandType uint8

const (
	ClientOperation RaftCommandType = iota
	ConfigChange
	ShardMigrate
	ShardGC
)

type RaftCommand struct {
	CmdType RaftCommandType
	Data    interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	Movein
	Moveout
	GC
)

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	ShardData      map[int]map[string]string
	DuplicateTable map[int64]LastOperationInfo
}
