package master

import (
	"time"

	"github.com/mapreduce_impl/common"
)

type WorkerInfo struct {
	// 标识&地址
	ID string
	Generation int
	Address  string // ip:port

	// 状态&存活
	Status   common.WorkerStatus
	LastPing time.Time // 超时检测

	// 执行的任务
	RunningTask   int
	CompletedTasks []int // 已完成的任务列表，如果该机器故障，Master 将重新分配所有已执行的任务
}

func NewWorkerInfo(id string, address string) WorkerInfo {
	return WorkerInfo{
		ID:       id,
		Generation: 1,
		Address:        address,
		Status:         common.WorkerStatusIdle,
		LastPing:       time.Time{},
		RunningTask:   -1,
		CompletedTasks: nil,
	}
}


func (wi *WorkerInfo) Reset() {
	wi.Generation++
	wi.Status = common.WorkerStatusIdle
	wi.LastPing = time.Time{}
	wi.RunningTask = -1
	wi.CompletedTasks = nil
}
