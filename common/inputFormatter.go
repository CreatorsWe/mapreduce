package common

import "fmt"


type InputFormatter struct {
	FilePath string
	From int   // bytes.
	To int
}


func NewInputFormatter(file_path string, from, to int) InputFormatter {
	return InputFormatter {
		FilePath: file_path,
		From: from,
		To: to,
	}
}


func (ift *InputFormatter) GetInfo() string {
	return fmt.Sprintf("(%s, %d, %d)", ift.FilePath, ift.From, ift.To)
}
