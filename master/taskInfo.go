package master

import (
	"log/slog"
	"time"

	"github.com/mapreduce_impl/common"
)

type MapTaskInfo struct {
	// 除了 id 和 mapTaskFormat 其他属性都可变
	ID               int
	MapTaskFormatter MapTaskFormatter

	// 任务状态
	Status common.TaskStatus

	// 执行的 worker 编号
	WorkerId string

	//开始时间和结束时间戳
	StartTime time.Time
	EndTime   time.Time

	IntermediatePaths []string
}

// 固定的输入、输出属性
type MapTaskFormatter struct {
	common.InputFormatter

	// 输出到 Worker 普通本地磁盘
	IntermediateDir string
	PartitionCount  int // 计算分区（输出文件）数量
}

func NewMaptaskFormatter(input_formatter common.InputFormatter, intermediate_dir string, partition_count int) MapTaskFormatter {
	return MapTaskFormatter{
		InputFormatter:  input_formatter,
		IntermediateDir: intermediate_dir,
		PartitionCount:  partition_count,
	}
}

func NewMapTaskInfo(id int, map_task_formatter MapTaskFormatter) MapTaskInfo {
	return MapTaskInfo{
		ID:                id,
		MapTaskFormatter:  map_task_formatter,
		Status:            common.TaskStatusIdle,
		WorkerId:          "",
		StartTime:         time.Time{},
		EndTime:           time.Time{},
		IntermediatePaths: nil,
	}
}

func (mti *MapTaskInfo) Reset() {
	mti.Status = common.TaskStatusIdle
	mti.WorkerId = ""
	mti.StartTime = time.Time{}
	mti.EndTime = time.Time{}
	mti.IntermediatePaths = nil
}

type ReduceTaskInfo struct {
	ID                  int
	ReduceTaskFormatter ReduceTaskFormatter

	// 任务状态
	Status common.TaskStatus

	// 执行 worker 编号
	WorkerId string

	//开始时间和结束时间戳
	StartTime time.Time
	EndTime   time.Time

	// 实际的输入、输出路径（动态确定）
	OutputPath string
}

// 固定的输入输出
type ReduceTaskFormatter struct {
	// 输入(文件地址 和 待处理的分区)
	PartitionIndex int

	IntermediatePaths []string

	// 输出
	OutputDir string
}

func NewReduceTaskFormatter(partition_index int, intermediate_paths []string, output_dir string) ReduceTaskFormatter {
	return ReduceTaskFormatter{
		PartitionIndex:    partition_index,
		IntermediatePaths: intermediate_paths,
		OutputDir:         output_dir,
	}
}

func NewReduceTaskInfo(id int, reduce_task_formatter ReduceTaskFormatter) ReduceTaskInfo {
	return ReduceTaskInfo{
		ID:                  id,
		ReduceTaskFormatter: reduce_task_formatter,
		Status:              common.TaskStatusIdle,
		WorkerId:            "",
		StartTime:           time.Time{},
		EndTime:             time.Time{},
		OutputPath:          "",
	}
}

func (rti *ReduceTaskInfo) Reset() {
	rti.Status = common.TaskStatusIdle
	rti.WorkerId = ""
	rti.StartTime = time.Time{}
	rti.EndTime = time.Time{}
}

func (mti *MapTaskInfo) DebugMapBriefTaskInfo() {
	slog.Debug("Map task brief information",
		"id", mti.ID,
		"status", mti.Status,
		"intermediate_dir", mti.MapTaskFormatter.IntermediateDir,
		"partition_count", mti.MapTaskFormatter.PartitionCount,
		"inptut_file", mti.MapTaskFormatter.FilePath,
		"from", mti.MapTaskFormatter.From,
		"size", mti.MapTaskFormatter.Size,
	)
}

func (mti *MapTaskInfo) DebugMapTaskInfo() {
	slog.Debug("Map task breif information",
		"id", mti.ID,
		"status", mti.Status,
		"worker_id", mti.WorkerId,
		"start_time", mti.StartTime,
		"end_time", mti.EndTime,
		"intermediate_files", mti.IntermediatePaths,
		"intermediate_dir", mti.MapTaskFormatter.IntermediateDir,
		"partition_count", mti.MapTaskFormatter.PartitionCount,
		"inptut_file", mti.MapTaskFormatter.FilePath,
		"from", mti.MapTaskFormatter.From,
		"size", mti.MapTaskFormatter.Size,
	)
}

func (rti *ReduceTaskInfo) DebugReduceBriefTaskInfo() {
	slog.Debug("Reduce task brief information",
		"id", rti.ID,
		"status", rti.Status,
		"partition_index", rti.ReduceTaskFormatter.PartitionIndex,
		"output_dir", rti.ReduceTaskFormatter.OutputDir,
		"intermediate_files", rti.ReduceTaskFormatter.IntermediatePaths,
	)
}

func (rti *ReduceTaskInfo) DebugReduceTaskInfo() {
	slog.Debug("Reduce task information",
		"id", rti.ID,
		"status", rti.Status,
		"worker_id", rti.WorkerId,
		"start_time", rti.StartTime,
		"end_time", rti.EndTime,
		"intermediate_files", rti.ReduceTaskFormatter.IntermediatePaths,
		"partition_index", rti.ReduceTaskFormatter.PartitionIndex,
		"output_dir", rti.ReduceTaskFormatter.OutputDir,
		"output_file", rti.OutputPath,
	)
}
