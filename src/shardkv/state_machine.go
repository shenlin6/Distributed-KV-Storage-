package shardkv

type MemoryKVStateMachine struct {
	KV map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

// Put 覆盖写入
func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	mkv.KV[key] = value
	return OK
}

// Append 追加写入
func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	mkv.KV[key] += value
	return OK
}
