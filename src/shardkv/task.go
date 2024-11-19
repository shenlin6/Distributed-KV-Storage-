package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) applyTask() {
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

				var opReply *OpReply
				raftCommand := message.Command.(RaftCommand)

				// 处理用户端信息
				if raftCommand.CmdType == ClientOperation {
					// 取出用户的操作信息
					op := raftCommand.Data.(Op)

					//去重（重复的客户端 putappend 请求）
					if op.OpType != OpGet && kv.isDuplicated(op.ClientId, op.SeqId) {
						opReply = kv.deduplicateTable[op.ClientId].Reply
					} else {
						//应用到状态机中

						sharId := key2shard(op.Key)
						opReply = kv.applyToStateMachine(op, sharId)
						//保存一下已经应用到状态机中的请求
						if op.OpType != OpGet {
							kv.deduplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: opReply,
							}
						}
					}
				} else { //处理 raft 端信息
					kv.handleConfigChangeMessage(raftCommand)
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

// 从 shardctl中获取当前配置
func (kv *ShardKV) getConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
			kv.mu.Unlock()

			//传入 raft 进行同步
			kv.ConfigCommand(RaftCommand{CmdType: ConfigChange, Data: newConfig}, &OpReply{})
		}

		time.Sleep(GetConfigInternal)
	}
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			// 拿到 shard 原来所属的 groupId
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

// GetShardsData 获取 shard 的数据
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 从 Leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 获取到的配置不是我们需要的
	if kv.currentConfig.Num != args.ConfigNum {
		reply.Err = ErrConfigNotReady
		return
	}

	// 拷贝 shard 数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	// 拷贝去重表,保证最终一致性
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, op := range kv.deduplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			//找到需要迁移的 shard
			kv.mu.Lock()
			gidToShards := kv.getShardByStatus(Movein)
			var wg sync.WaitGroup
			for gid, sharIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, sharIds []int) {
					defer wg.Done()
					// 遍历 group 中的每个节点，从 Leader 当中读取到对应的 shard 数据
					getShardArgs := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  sharIds,
					}
					for _, server := range servers {
						var getSharReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getSharReply)

						// 获取到了 shard 数据，开始 shard 迁移
						if ok && getSharReply.Err == OK {
							kv.ConfigCommand(RaftCommand{ShardMigrate, getSharReply}, &OpReply{})
						}
					}

				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, sharIds)
			}

			kv.mu.Unlock()
			wg.Wait()

		}
		time.Sleep(ShardMigration)
	}
}
