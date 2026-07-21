package common


type WorkerStatus int

const (
	WorkerStatusIdle     = iota // 空闲，未执行任何任务
	WorkerStatusBusy            // 繁忙，正在执行任务
	WorkerStatusDead            // 已失效（心跳超时或主动退出）
	WorkerStatusShutDown        // 已关闭
)

type TaskStatus int

const (
	TaskStatusIdle = iota
	TaskStatusRunning
	TaskStatusCompleted
	TaskStatusFatal
)

type KB int64

// 模拟大文件, 本质是一个目录
type BigFile struct {
	NodeRecord NodeRecord
	TotalSize int// 假设单位是 KB
} 


type NodeRecord struct {
	DataNode string
	Block int
	StartIndex int
	Size int
}

