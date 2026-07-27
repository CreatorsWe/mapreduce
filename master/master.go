package master

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"net"

	"github.com/google/uuid"
	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
	"github.com/mapreduce_impl/utility"
	"google.golang.org/grpc" 
)

/*
go 无法获取 map 值的引用，所以涉及到修改值数据的场景必须存储指针类型。
*/
type Master struct {
	ID string
	Address string


	NumMapTask int
	NumReduceTask int
	TotalNumWorker int
	InputFile string
	IntermediateDir string
	OutputDir string

	// worker information
	Workers map[string]*WorkerInfo // Worker 信息
	CurrentNumWorker int

	// task information
	Phase atomic.Bool  // true 表示 MAP；false 表示 REDUCE
	MapTasks    map[int]*MapTaskInfo // 任务信息
	ReduceTasks map[int]*ReduceTaskInfo

	TaskRecord int   // 仅用于得到唯一的任务编号
	WorkerMut sync.Mutex
	TaskMut sync.Mutex
	pb.UnimplementedMasterServiceServer
}




// 初始化固定信息
func NewMaster(address string) Master {
	return Master{
		ID:                      uuid.NewString(),
		Address:                   address,
		Workers: make(map[string]*WorkerInfo),
		MapTasks: make(map[int]*MapTaskInfo),
		ReduceTasks: make(map[int]*ReduceTaskInfo),
	}
}

// 初始化作用信息
func (master *Master) JobInitialzation() {
	master.TotalNumWorker = common.NumWorker
	master.InputFile = common.InputDir
	master.NumReduceTask = common.NReduce

	// 初始化 Map 作业
	master.Phase.Store(true)
	master.InitMapJob()
	master.InitReduceJob()
}

func (master *Master) InitMapJob() {
	log.Printf("[task initialzation] Start initialzing Map tasks\n")
	// 计算 NumMapTask，Map 任务数量
	master.NumMapTask = utility.NumInputFile()
	// 初始化所有 Map 任务
	for range master.NumMapTask {
		inputFile := utility.GetInputFile()
		mapTaskFormat := NewMaptaskFormat(inputFile, master.IntermediateDir, master.NumReduceTask)
		mapTaskInfo := NewMapTaskInfo(master.TaskRecord, mapTaskFormat)
		master.MapTasks[master.TaskRecord] = &mapTaskInfo
		master.TaskRecord++
	}
	log.Printf("[task initialzation] End initialzing Map tasks\n")
	if common.IsDebug {
		log.Printf("[debug] initialzed Map tasks\n")
		for _, mapTask := range master.MapTasks {
			log.Printf("[debug] %s\n", GetInitMapTaskInfo(*mapTask))
		}
	}
}


func (master *Master) InitReduceJob() {
	log.Printf("[task initialzation] Start initialzing Reduce tasks\n")
	for i := range master.NumReduceTask {
		reduceTaskFormat := NewReduceTaskFormat(i, master.OutputDir)
		reduceTaskInfo := NewReduceTaskInfo(master.TaskRecord, reduceTaskFormat)
		master.ReduceTasks[master.TaskRecord] = &reduceTaskInfo
		master.TaskRecord++
	}	
	log.Printf("[task initialzation] End initialzing Reduce tasks\n")
	if common.IsDebug {
		log.Printf("[debug] initialzed Reduce tasks\n")
		for _, reduceTask := range master.ReduceTasks {
			log.Printf("[debug] %s\n", GetInitReduceTaskInfo(*reduceTask))
		}
	}
}


// 启动 Master 服务
func (master *Master) StartService() {
	ctx := context.Background()	
	master.JobInitialzation()
	var wg sync.WaitGroup
	wg.Go(func() {
		master.RpcServiceCall(ctx)
	})
	master.WaitEnoughWorker(ctx)
	wg.Wait()
}



func (master *Master) WaitEnoughWorker(ctx context.Context) {
	for {
		if master.CurrentNumWorker >= master.TotalNumWorker {
			break
		}
	}
	log.Printf("[system] We get enough workers\n")

}






func (master *Master) RpcServiceCall(ctx context.Context) {
	// 1. 创建 gRPC 实例
	grpcServer := grpc.NewServer()

	// 2. 注册服务
	pb.RegisterMasterServiceServer(grpcServer, master)

	// 3. 指定端口上创建 TCP 监听器监听服务
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("[TCP Service] TCP service start error")
	}

	err = grpcServer.Serve(listen)  // 内部调用 Accept() 阻塞代码
	if err != nil {
		log.Fatal("[TCP Service] gRPC listen port error")
	}
	log.Println("[TCP Service] TCP service listening  on port 50051")
	
	// 获取任务

}
