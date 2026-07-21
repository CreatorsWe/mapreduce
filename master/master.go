package master

import (
	"context"
	"log"
	"math"
	"net"
	"sync"

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

func (master *Master) WorkerRegister(tx context.Context, req *pb.WorkerRegisterRequest) (*pb.WorkerRegisterResponse, error) {
	WorkerInfo := NewWorkerInfo(req.Uuid, req.Address)
	master.WorkerMut.Lock()

	master.Workers = append(master.Workers, WorkerInfo)
	master.CurrentNumWorkers++

	master.WorkerMut.Unlock()

	log.Printf("[Worker Register] Worker %s registered in %s.", req.Uuid, req.Address)

	return &pb.WorkerRegisterResponse{
		Ok:       true,
		MasterId: master.UUID,
		NReduce:  int32(master.NReduce), // why does worker need NReduce?
	}, nil
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

