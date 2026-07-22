package master

import (
	"time"

	"github.com/mapreduce_impl/common"
)

type MapTaskInfo struct {
	// 除了 id 和 mapTaskFormat 其他属性都可变
	ID int
	MapTaskFormat MapTaskFormat 

	// 任务状态
	Status common.TaskStatus

	// 执行的 worker 编号
	WorkerID string

	//开始时间和结束时间戳
	StartTime time.Time
	EndTime   time.Time

	InterMediateAddresses []string
}

// 固定的输入、输出属性
type MapTaskFormat struct {

	// 普通文件系统，一个 Map 任务解析一个文件
	InputFile string

	// 输出到 Worker 普通本地磁盘
	IntermediateDir string
	NReduce         int // 计算分区（输出文件）数量
}

func NewMaptaskFormat(input_file, intermediate_dir string, n_reduce int) MapTaskFormat {
	return MapTaskFormat {
		InputFile: input_file,
		IntermediateDir: intermediate_dir,
		NReduce: n_reduce,
	}	
}
func NewMapTaskInfo(id int, map_task_format MapTaskFormat) MapTaskInfo { 
	return MapTaskInfo {
		ID: id,
		MapTaskFormat: map_task_format,
		Status: common.TaskStatusIdle,
		WorkerID: "",
		StartTime: time.Time{},
		EndTime: time.Time{},
		InterMediateAddresses: nil,
	}
}


type ReduceTaskInfo struct {
	ID int
	ReduceTaskFormat ReduceTaskFormat

	// 任务状态
	Status common.TaskStatus

	// 执行 worker 编号
	WorkerID string

	//开始时间和结束时间戳
	StartTime time.Time
	EndTime time.Time

	// 实际的输入、输出路径（动态确定）
	InterMediateAddresses []string
	OutputPath string
}

// 固定的输入输出
type ReduceTaskFormat struct {
	// 输入(文件地址 和 待处理的分区)
	PartitionIndex int

	// 输出
	OutputDir string
}

func NewReduceTaskFormat(partition_index int, output_dir string) ReduceTaskFormat {
	return ReduceTaskFormat {
		PartitionIndex: partition_index,
		OutputDir: output_dir,
	}
}

func NewReduceTaskInfo(id int, reduce_task_format ReduceTaskFormat) ReduceTaskInfo { 
	return ReduceTaskInfo {
		ID: id,
		ReduceTaskFormat: reduce_task_format,
		Status: common.TaskStatusIdle,
		WorkerID: "",
		StartTime: time.Time{},
		EndTime: time.Time{},
		InterMediateAddresses: nil,
		OutputPath: "",
	}
}
