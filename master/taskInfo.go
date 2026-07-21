package master

import (
	"time"

	"github.com/mapreduce_impl/common"
)

type MapTaskInfo struct {
	ID int

	// 任务状态
	Status common.TaskStatus

	// 输入(大型文件系统通常分块存储，需要按偏移读取)
	DataNode string
	Block int
	StartIndex   int
	Size  int // EndOfset - StartOffset

	// 输出到 Worker 普通本地磁盘
	IntermediateDir string
	NReduce         int // 计算分区（输出文件）数量

	// 执行 worker 编号
	WorkerID string

	//开始时间和结束时间戳
	StartTime time.Time
	EndTime   time.Time
}

func NewMapTaskInfo(_id int, _nodeRecord common.NodeRecord, _nReduce int, _intermediateDir string) MapTaskInfo {
	return MapTaskInfo {
		ID: _id,
		Status: common.TaskStatusIdle,
		DataNode: _nodeRecord.DataNode,
		Block: _nodeRecord.Block,
		StartIndex: _nodeRecord.StartIndex,
		Size: _nodeRecord.Size,
		IntermediateDir: _intermediateDir,
		NReduce: _nReduce,
		WorkerID: "",
		StartTime: time.Time{},
		EndTime: time.Time{},
	}
}

func (mapTaskInfo *MapTaskInfo) CheckSafety() bool {
	if (len(mapTaskInfo.DataNode) == 0 || len(mapTaskInfo.IntermediateDir) == 0 || len(mapTaskInfo.WorkerID) == 0) {
		return false
	}
	return true
}

type ReduceTaskInfo struct {
	ID int

	// 任务状态
	Status common.TaskStatus

	// 输入(文件地址 和 待处理的分区)
	ReduceIndex int

	// 输出
	OutputDir string

	// 执行 worker 编号
	WorkerID string

	//开始时间和结束时间戳
	PullDataStartTime time.Time
	PullDataEndTime   time.Time
	ReduceStartTime   time.Time
	ReduceEndTime     time.Time
}


func NewReduceTaskInfo(_id int, _reduceIndex int, _outputDir string) ReduceTaskInfo {
	return ReduceTaskInfo {
		ID: _id,
		Status: common.TaskStatusIdle,
		ReduceIndex: _reduceIndex,
		OutputDir: _outputDir,
		WorkerID: "",
		PullDataStartTime: time.Time{},
		PullDataEndTime: time.Time{},
		ReduceStartTime: time.Time{},
		ReduceEndTime: time.Time{},
	}
}
