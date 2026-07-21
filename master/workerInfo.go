package master

import (
	"time"

	"github.com/mapreduce_impl/common"
)

type WorkerInfo struct {
	// 标识&地址
	WorkerID string
	Address  string // ip:port

	// 状态&存活
	Status   common.WorkerStatus
	LastPing time.Time // 超时检测

	// 执行的任务
	RunningTasks   []int
	CompletedTasks []int // 已完成的任务列表，如果该机器故障，Master 将重新分配所有已执行的任务
}

func NewWorkerInfo(WorkerID, Address string) WorkerInfo {
	return WorkerInfo{
		WorkerID:       WorkerID,
		Address:        Address,
		Status:         common.WorkerStatusIdle,
		LastPing:       time.Time{},
		RunningTasks:   nil,
		CompletedTasks: nil,
	}
}
