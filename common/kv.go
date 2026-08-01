package common

import (
	"fmt"
	"iter"
	"regexp"
	"strings"
)

// 键值对类型
type KV struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func (kv *KV) ToJSON() string {
	return fmt.Sprintf("{ %s, %s }", kv.Key, kv.Value)
}

func FromJSON(record string) KV {
	re := regexp.MustCompile(`\{ ([^,]+), ([^}]+) \}`)
	matches := re.FindStringSubmatch(record)
	return NewKV(matches[1], matches[2])
}


func NewKV(k, v string) KV {
	return KV{
		Key:   k,
		Value: v,
	}
}

type ListKV []KV

func (list_kv ListKV) ToJSONs() string {
	var content []string
	for _, kv := range list_kv {
		content = append(content, kv.ToJSON())
	}
	return strings.Join(content, "\n")

}

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

func (listKV ListKV) Iter() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, kv := range listKV {
			if !yield(kv.Key, kv.Value) {
				return
			}
		}
	}
}
