package worker

// 键值对类型
type KV struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func NewKV(k, v string) KV {
	return KV{
		Key:   k,
		Value: v,
	}
}

type ListKV []KV

// 一般输出键值对列表，需要获取其长度，因为需要排序，所以需要实现 Swap 和 Less 函数
func (l ListKV) Len() int {
	return len(l)
}

func (l ListKV) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l ListKV) Less(i, j int) bool {
	return l[i].Key < l[j].Key
}
