package common

import "fmt"


type InputFormatter struct {
	FilePath string
	From int   // bytes.
	Size int
}


func NewInputFormatter(file_path string, from, size int) InputFormatter {
	return InputFormatter {
		FilePath: file_path,
		From: from,
		Size: size,
	}
}


func (ift *InputFormatter) GetInfo() string {
	return fmt.Sprintf("(%s, %d, %d)", ift.FilePath, ift.From, ift.Size)
}
