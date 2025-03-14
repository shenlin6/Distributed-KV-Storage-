package shardkv

type MemoryKVStateMachine struct {
	KV     map[string]string
	Status ShardStatus
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
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

func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string)
	for k, v := range mkv.KV {
		newKV[k] = v
	}
	return newKV
}
