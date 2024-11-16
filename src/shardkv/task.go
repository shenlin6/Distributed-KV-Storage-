package shardkv

import "time"

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

// 从 shardctl中获取当前配置
func (kv *ShardKV) getConfigTask() {
	for !kv.killed() {
		kv.mu.Lock()
		newConfig := kv.mck.Query(-1)
		kv.currentConfig = newConfig
		kv.mu.Unlock()
		time.Sleep(GetConfigInternal)
	}
}
