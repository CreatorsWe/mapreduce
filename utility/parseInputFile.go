package utility

import (
	"os"

	"github.com/mapreduce_impl/common"
)


var inputFiles []string
var inputFileIndex int = 0

func init() {
	if _, err := os.Stat(common.InputDir); err != nil {
		inputFiles = nil
		return
	}

	entries, err := os.ReadDir(common.InputDir)
	if err != nil {
		inputFiles = nil
		return
	}
	for _, entry := range entries {
		inputFiles = append(inputFiles, entry.Name())
	}
	inputFileIndex = 0
}


func NumInputFile() int {
	if inputFiles == nil { return 0 }
	return len(inputFiles)
}


func GetInputFile() string {
	current_index := inputFileIndex
	inputFileIndex++
	return inputFiles[current_index]
}





