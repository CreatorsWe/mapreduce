package master

import (
	"context"
	"log"
	"math"
	"net"
	"sync"
	"time"
	"fmt"

	"github.com/google/uuid"
	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
	"google.golang.org/grpc"
)

// worker vector
// mapTask queue
// reduceTask queue
// numWorkers
// totalWorkers
// enoughWorkers chan bool ? 分布式机器需要这个通信吗？
// crashChan 错误
// mux 锁，需要访问同一资源
type Master struct {
	UUID string

	Address string
	// Just record the brief information of workers, not complete worker struct.
	Workers []WorkerInfo // Worker 信息

	NReduce int // 分区数量

	MapTasks    []MapTaskInfo // 任务信息
	ReduceTasks []ReduceTaskInfo

	CurrentNumWorkers int // 当前 Worker 数量
	TotalWorkers      int // 需要的总 Worker 数量

	CurrentNumWorkerForMap    int // 执行 Map 任务的 Worker 数量
	CurrentNumWorkerForReduce int // 执行 Reduce 任务的 Worker 数量

	WorkerMut sync.Mutex

	pb.UnimplementedMasterServiceServer
}

// 输入文件姑且算作普通文件系统
func NewMaster(totalWorkers, nMap, nReduce int, address string, inputFile string) Master {
	return Master{
		UUID:                      uuid.NewString(),
		Address:                   address,
		Workers:                   nil,
		NReduce:                   nReduce,
		MapTasks:                  nil,
		ReduceTasks:               nil,
		CurrentNumWorkers:         0,
		TotalWorkers:              totalWorkers,
		CurrentNumWorkerForMap:    0,
		CurrentNumWorkerForReduce: 0,
		WorkerMut:                 sync.Mutex{},
	}
}

// 启动 Master 服务
func (master *Master) StartService() {
	// 1. 创建 gRPC 实例
	grpcServer := grpc.NewServer()

	// 2. 注册服务
	pb.RegisterMasterServiceServer(grpcServer, master)

	// 3. 指定端口上创建 TCP 监听器监听服务
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("[TCP Service] TCP service start error")
	}

	err = grpcServer.Serve(listen)
	if err != nil {
		log.Fatal("[TCP Service] gRPC listen port error")
	}
	log.Println("[TCP Service] TCP service listening  on port 50051")

}

// service MasterService {
//   rpc WorkerRegister(WorkerRegisterRequest) returns (WorkerRegisterReply);
//   rpc Heartbeat(HeartbeatRequest) returns (HeartbeatReply);
// }
func (master *Master) WorkerRegister(tx context.Context, req *pb.WorkerRegisterRequest) (*pb.WorkerRegisterReply, error) {
	WorkerInfo := NewWorkerInfo(req.Uuid, req.Address)
	master.WorkerMut.Lock()

	master.Workers = append(master.Workers, WorkerInfo)
	master.CurrentNumWorkers++

	master.WorkerMut.Unlock()

	log.Printf("[Worker Register] Worker %s registered in %s.", req.Uuid, req.Address)

	return &pb.WorkerRegisterReply{
		Ok:       true,
		MasterId: master.UUID,
	}, nil
}

func (master *Master) Heartbeat(tx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	// 1. 根据 id 找到 workers
	var worker *WorkerInfo
	for _, w := range master.Workers {
		if w.WorkerID == req.Uuid {
			worker = &w
			break
		}
	}

	// 2. 更新 LastTime
	worker.LastPing = time.Now()

	// 3. 如果申请任务，返回任务并更新 map 信息（不应该这么草率，希望后面改进）
	if req.RequestTask {
		master.UpdateMasterOnTaskComplete(worker.WorkerID, int(req.CurrentTask), req.CurrentTaskType)	
	} 
}

func (master *Master) TaskInitialization(_jobInput common.BigFile, _blockSize int, _intermediateDir, _outputDir string) {
	// compute M which the number of map task
	M := int(math.Ceil(float64(_jobInput.TotalSize / _blockSize)))

	idCount := 0

	// 初始化 Map 任务
	for range M {
		mapTaskInfo := NewMapTaskInfo(idCount, _jobInput.NodeRecord, master.NReduce, _intermediateDir)
		master.MapTasks = append(master.MapTasks, mapTaskInfo)
		idCount++
	}

	// 初始化 Reduce 任务
	for i := range master.NReduce {
		reduceTaskInfo := NewReduceTaskInfo(idCount, i, _outputDir)
		master.ReduceTasks = append(master.ReduceTasks, reduceTaskInfo)
		idCount++
	}
}

// 当一个 task 完成时，需要更新： 1. 执行它的 worker 信息； 2. 更新这个 task 的信息；
func (master *Master) UpdateMasterOnTaskComplete(workerID string, taskID int, taskType string) error {
	// 1. 判断 taskID 是否是这个 worker 正在执行的 task	
	var workerInfo *WorkerInfo = nil
	var taskInfo any
	for _, w := range master.Workers {
		if w.WorkerID == workerID {
			workerInfo = &w
			break
		}
	}

	switch taskType {
	case "map":
		for _, t := range master.MapTasks {
			if t.ID == taskID {
				taskInfo = &t
				break
			}
		}
	case "reduce":
		for _, t := range master.ReduceTasks {
			if t.ID == taskID {
				taskInfo = &t
				break
			}
		}
	}

	if workerInfo == nil || taskInfo == nil {
		return fmt.Errorf("[Update Master]Do not find worker %s from master's workers", workerID)
	}

	// 2. 更新 workers
	workerInfo.Status = common.WorkerStatusIdle
	workerInfo.CompletedTasks = append(workerInfo.CompletedTasks, taskID)
	
	// 3. 更新 tasks
	switch t := taskInfo.(type) {
	case MapTaskInfo:
		t.Status = common.TaskStatusCompleted
		t.EndTime = time.Now()
	case ReduceTaskInfo: 
		t.Status = common.TaskStatusCompleted
		t.EndTime = time.Now()	
	}	
	return nil
}
