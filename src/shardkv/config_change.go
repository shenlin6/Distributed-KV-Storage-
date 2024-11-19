package shardkv

import (
	"6.824/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	// 将操作存储在 raft 日志中并同步给follower
	index, _, isLeader := kv.rf.Start(command)

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

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	default:
		panic("unknow config change type")
	}
}

// 处理配置变更
func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	// 判断拉取的编号是否匹配group想要的配置编号
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			// 某个 shard 当前不属于某个 group ,配置变更后属于，则将 shard 迁移进来
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = Movein
				}
			}

			// 某个 shard 当前属于某个 group ,配置变更后不属于，则将 shard 迁移出去
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = Moveout
				}
			}
		}
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{
			Err: OK,
		}
	}
	return &OpReply{Err: ErrWrongConfig}
}
