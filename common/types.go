package common


type WorkerStatus int

const (
	WorkerStatusIdle     = iota // 空闲，未执行任何任务
	WorkerStatusBusy            // 繁忙，正在执行任务
	WorkerStatusDead            // 已失效（心跳超时或主动退出）
	WorkerStatusShutdown        // 已关闭
)

type TaskStatus int

const (
	TaskStatusIdle = iota
	TaskStatusRunning
	TaskStatusCompletion
	TaskStatusFatal
)


// 模拟大文件, 本质是一个目录
// 一个 Map 任务处理一个文件，即一个文件就是一个 Block
// DataNode 是目录路径，Block 是目录下的文件索引，startIndex 为 0，size 默认为 0 或者一个极大值（反正需要读取文件所有内容）
// type BigFile struct {
// 	NodeRecords []NodeRecord
// 	TotalSize int
// } 
//
//
// type NodeRecord struct {
// 	DataNode string
// 	Block int
// 	StartIndex int
// 	Size int
// }
//
//
// // 提供一个统一的全局状态管理 block 对应的文件
// func init()
