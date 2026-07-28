package mrapps

import (
	"strconv"
	"strings"
	"unicode"
	. "github.com/mapreduce_impl/common"
)

// 纯函数，不依赖外部状态
func Map(filename string, contents string) ListKV {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	
	words := strings.FieldsFunc(contents, ff)

	var listKV ListKV

	for _ ,word := range words {
		listKV = append(listKV, NewKV(word, "1"))
	}

	return listKV
}


func Reduce(key string, values []string) KV {
	return NewKV(key, strconv.Itoa(len(values)))
}
