package worker

import "github.com/mapreduce_impl/common"

type MapTaskInfo struct {
	taskID          int
	inputFormatter  common.InputFormatter
	partitionCount  int
	intermediateDir string
}

func NewMapTaskInfo(task_id int, input_formatter common.InputFormatter, parition_count int, intermediate_dir string) MapTaskInfo {
	return MapTaskInfo{
		taskID:          task_id,
		inputFormatter:  input_formatter,
		partitionCount:  parition_count,
		intermediateDir: intermediate_dir,
	}
}

type ReduceTaskInfo struct {
	taskID         int
	partitionIndex int
	partitionPaths []string
	outputDir      string
}

func NewReduceTaskInfo(task_id int, partition_index int, partition_paths []string, output_dir string) ReduceTaskInfo {
	return ReduceTaskInfo{
		taskID:         task_id,
		partitionIndex: partition_index,
		partitionPaths: partition_paths,
		outputDir:      output_dir,
	}
}
