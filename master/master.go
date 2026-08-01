package master

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"net"
	"context"
	"time"
	"fmt"

	"github.com/google/uuid"
	. "github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
	"google.golang.org/grpc" 
)

/*
go 无法获取 map 值的引用，所以涉及到修改值数据的场景必须存储指针类型。
*/
type Master struct {
	ID string
	Address string


	MapTaskCount int     // 一旦初始化，只读
	ReduceTaskCount int  // 一旦初始化，只读
	TotalWorkerCount int // 只读
	CurrentAvailableWorkerCount atomic.Int32  // 注册/失联时动态改变
	
	InputFiles []string
	IntermediateDir string
	OutputDir string

	BlockSize int

	// worker information
	Workers map[string]*WorkerInfo // Worker 信息

	// task information
	MapTasks    map[int]*MapTaskInfo // 任务信息
	ReduceTasks map[int]*ReduceTaskInfo

	// Map task output files
	PartitionMaps map[int][]string
	
	Phase atomic.Bool  // true 表示 MAP；false 表示 REDUCE

	// 超时检测
	Timeout time.Duration
	Periodic time.Duration

	TaskRecord int   // 仅用于得到唯一的任务编号
	WorkerMut sync.RWMutex
	TaskMut sync.RWMutex

	// 是否获取足够的 workerNum
	enoughWorker atomic.Bool
	pb.UnimplementedMasterServiceServer
}



// 周期检测
func (master *Master) CheckTimeOutPeriodically(ctx context.Context) {
	for {
		select {
		case <- ctx.Done():
			slog.Debug("exit periodic time-out check")
			return
		default:
			current := time.Now()

			master.WorkerMut.Lock()
			defer master.WorkerMut.Unlock()

			for _, worker := range master.Workers {
				if current.Sub(worker.LastPing) > master.Timeout {
					worker.Status = WorkerStatusDead   // 外部获取锁，这里不能调用 UpdateWorkerStatus，因为其内部也需要获取锁
				}	
			}

			time.Sleep(master.Periodic)
		}
	}
}



func (master *Master) RegisterWorker(id string, generation int, address string) (int, MasterError) {
	exist, err := master.existWorker(id, generation)

	if !err.IsNil() { return -1, err }

	// 当前 worker 数量 + 1
	master.CurrentAvailableWorkerCount.Add(1)

	if !exist {
		workerInfo := NewWorkerInfo(id, address)
		master.WorkerMut.Lock()
		master.Workers[id] = &workerInfo
		master.WorkerMut.Unlock()

		return 1, NewMasterNilError()
	} else {
		master.WorkerMut.Lock()
		master.Workers[id].Reset()
		gen := master.Workers[id].Generation
		master.WorkerMut.Unlock()
		return gen, NewMasterNilError()
	}
}




func (master *Master) CheckWorker(id string, generation int) MasterError {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RUnlock()

	workerInfo, exist := master.Workers[id]

	if !exist { return NewMasterError(WorkerNotExist, "worker %s do not eixst", id) } 

	if workerInfo.Generation != generation { return NewMasterError(WorkerNoSameGeneration, "worker %s has no same generation", id) }

	// 检查 Status 是否为 Dead 或 Shutdown
	if workerInfo.Status ==	WorkerStatusDead { return NewMasterError(WorkerDead, "worker %s is dead", id)}
	if workerInfo.Status == WorkerStatusShutdown { return NewMasterError(WorkerShutdown, "worker %s shutdown", id)}

	return NewMasterNilError()
}




func (master *Master) UpdateWorkerLastPing(id string, last_ping time.Time) {
	master.WorkerMut.Lock()
	defer master.WorkerMut.Unlock()
	master.Workers[id].LastPing = last_ping
}




func (master *Master) UpdateWorkerStatus(id string, status WorkerStatus) {
	master.WorkerMut.Lock()
	defer master.WorkerMut.Unlock()
	master.Workers[id].Status = status
}



// 不检查 worker 是否合法，调用前必须进行 CheckWorker 检查
func (master *Master) UpdateWorkerRunningTask(id string, running_task_id int) {
	master.WorkerMut.Lock()
	defer master.WorkerMut.Unlock()
	master.Workers[id].RunningTask = running_task_id
}




func (master *Master) AppendWorkerComletionTask(id string, completed_task_id int) {
	master.WorkerMut.Lock()
	defer master.WorkerMut.Unlock()
	master.Workers[id].CompletedTasks = append(master.Workers[id].CompletedTasks, completed_task_id)
}




func (master *Master) GetWorkerStatus(id string) WorkerStatus {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RLocker()
	status := master.Workers[id].Status
	return status
}




func (master *Master) GetWorkerLastPing(id string) time.Time {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RUnlock()
	lastPing := master.Workers[id].LastPing
	return lastPing
}




func (master *Master) GetWorkerRunningTask(id string) int {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RUnlock()
	runningTask := master.Workers[id].RunningTask
	return runningTask
}




func (master *Master) GetWorkerCompledTasks(id string) []int {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RUnlock()
	completedTasks := master.Workers[id].CompletedTasks
	return completedTasks
}




// Task 处理函数
func (master *Master) GetIdleMapTaskId() (int, bool) {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	for mapTaskId, mapTaskInfo := range master.MapTasks {
		if mapTaskInfo.Status == TaskStatusIdle {
			return mapTaskId, true
		}
	}

	return -1, false
}




func (master *Master) UpdateMapTaskForDivice(task_id int, worker_id string) {
	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	task, exist := master.MapTasks[task_id]

	if !exist { panic("It must be a existed task's id") }

	task.WorkerId = worker_id
	task.Status = TaskStatusRunning
	task.StartTime = time.Now()
}




func (master *Master) UpdateMapTaskForCompletion(task_id int, worker_id string, intermediate_paths []string) {
	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	task, exist := master.MapTasks[task_id]

	if !exist { panic("It must be a existed task's id") }

	if worker_id != task.WorkerId { panic("the worker id that the MapTasks record is not equal to the requesting worker id") } 

	task.Status = TaskStatusCompletion
	task.EndTime = time.Now()
	task.IntermediatePaths = intermediate_paths
}




func (master *Master) GetPbMapTaskInfoForDivice(task_id int) *pb.MapTaskInfo {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	taskInfo, exist := master.MapTasks[task_id]

	if !exist { panic("It must be a existed task's id") }

	pbInputFormatter := pb.InputFormatter {
		InputPath: taskInfo.MapTaskFormatter.FilePath,
		From: int32(taskInfo.MapTaskFormatter.From),
		To: int32(taskInfo.MapTaskFormatter.Size),
	}

	pbMapTaskInfo := pb.MapTaskInfo{
		InputFormatter: &pbInputFormatter,
		PartitionCount: int32(taskInfo.MapTaskFormatter.PartitionCount),
		IntermediateDir: taskInfo.MapTaskFormatter.IntermediateDir,
	}

	return &pbMapTaskInfo
}




func (master *Master) GetPbReduceTaskInfoForDivice(task_id int) *pb.ReduceTaskInfo {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	taskInfo, exist := master.ReduceTasks[task_id]

	if !exist { panic("It must be a existed task's id") }

	pbReduceTaskInfo := pb.ReduceTaskInfo {
		PartitionIndex: int32(taskInfo.ReduceTaskFormatter.PartitionIndex),
		PartitionPaths: taskInfo.ReduceTaskFormatter.IntermediatePaths,
		OutputDir: master.OutputDir,
	}

	return &pbReduceTaskInfo
}




func (master *Master) GetIdleReduceTaskId() (int, bool) {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	for reduceTaskId, reduceTaskInfo := range master.ReduceTasks {
		if reduceTaskInfo.Status == TaskStatusIdle {
			return reduceTaskId, true
		}
	}

	return -1, false
}





func (master *Master) UpdateReduceTaskForDivice(task_id int, worker_id string) {
	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	task, exist := master.ReduceTasks[task_id]

	if !exist { panic("It must be a existed task's id") }

	task.WorkerId = worker_id
	task.Status = TaskStatusRunning
	task.StartTime = time.Now()
}




func (master *Master) UpdateReduceTaskForCompletion(task_id int, worker_id string, output_path string) {
	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	task, exist := master.ReduceTasks[task_id]

	if !exist { panic("It must be a existed tasks's id") }

	if worker_id != task.WorkerId { panic("the worker id that the ReduceTasks record is not equal to the requesting worker id") }

	task.Status = TaskStatusCompletion
	task.EndTime = time.Now()
	task.OutputPath = output_path
}




// 重置 worker 已执行的所有 task 的状态
func (master *Master) ResetTasksOfWorker(worker_id string) {
	var taskIds []int
	{
		master.WorkerMut.RLock()
		defer master.WorkerMut.RUnlock()

		worker, exist := master.Workers[worker_id]

		if !exist { panic("It must be a existing worker") }

		if worker.CompletedTasks == nil && worker.RunningTask == -1 { return }

		taskIds = append(taskIds, worker.CompletedTasks...)
		taskIds = append(taskIds, worker.RunningTask)
	}

	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()
	for _, taskId := range taskIds {
		mapTaskInfo, exist := master.MapTasks[taskId]
		if !exist { goto rt }
		mapTaskInfo.Reset()
		continue
		rt:
		reduceTaskInfo, exist := master.ReduceTasks[taskId]
		if !exist { panic(fmt.Sprintf("task %d do not exist", taskId)) }
		reduceTaskInfo.Reset()
	}
}



// 关闭所有 worker
func (master *Master) ShutdownAllWorker() {
	master.WorkerMut.Lock()
	defer master.WorkerMut.Unlock()

	for _, workerInfo := range master.Workers {
		workerInfo.Status = WorkerStatusShutdown
	}
}

// 初始化固定信息
func NewMaster(address string) Master {
	return Master{
		ID:                      uuid.NewString(),
		Address:                   address,
		Workers: make(map[string]*WorkerInfo),
		MapTasks: make(map[int]*MapTaskInfo),
		ReduceTasks: make(map[int]*ReduceTaskInfo),
		PartitionMaps: make(map[int][]string),
		Timeout: time.Duration(Timeout) * time.Second,
		Periodic: time.Duration(Periodic) * time.Second,
	}
}




// 启动 Master 服务
func (master *Master) StartService(){
	// 如果 grpc 服务启动失败, 关闭 waitEnoughWorker
	serviceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化状态信息和 Map 任务
	master.jobInitialzation()

	var wg sync.WaitGroup

	// gorouinte 等待足够的 worker
	wg.Go(func() { 
		master.waitEnoughWorker(serviceCtx) 

		time.Sleep(5 * time.Second)

		master.CheckTimeOutPeriodically(serviceCtx)
	})

	// 主线程 grpc 服务阻塞监听 50051 端口
	err := master.rpcServiceCall(serviceCtx)

	if !err.IsNil() { 
		panic(err.String())
	}

	wg.Wait()
}






// 初始化作用信息
func (master *Master) jobInitialzation() {
	master.TotalWorkerCount = WorkerCount
	master.InputFiles = InputFiles
	master.ReduceTaskCount = ReduceCount
	master.IntermediateDir = IntermediateDir
	master.OutputDir = OutputDir
	master.BlockSize = BlockSize

	master.CurrentAvailableWorkerCount.Store(0)

	// 初始化 Map 作业
	master.Phase.Store(true)
	master.initMapJob()
}


func (master *Master) initMapJob() {
	slog.Info("Init Map job")

	inputFormatters, err := DivideToInputFormatters(master.InputFiles, master.BlockSize)	

	if err != nil { panic(err.Error()) }

	master.MapTaskCount = len(inputFormatters)

	// 初始化所有 Map 任务
	for _ , inputFormatter := range inputFormatters {
		mapTaskFormat := NewMaptaskFormatter(inputFormatter, master.IntermediateDir, master.ReduceTaskCount)
		mapTaskInfo := NewMapTaskInfo(master.TaskRecord, mapTaskFormat)
		master.MapTasks[master.TaskRecord] = &mapTaskInfo
		master.TaskRecord++

		mapTaskInfo.DebugMapBriefTaskInfo()
	}
}


func (master *Master) InitReduceJob() {
	if master.Phase.Load() { panic("Trying to initialze the Reduce tasks when the Phase is true") }

	slog.Info("Init Reduce job")

	master.initPartitionMaps()

	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	for i := range master.ReduceTaskCount {
		partitionFiles, exist := master.PartitionMaps[i]

		if !exist { panic(fmt.Sprintf("There is no intermediate files in partition %d", i)) }

		reduceTaskFormat := NewReduceTaskFormatter(i, partitionFiles, master.OutputDir)
		reduceTaskInfo := NewReduceTaskInfo(master.TaskRecord, reduceTaskFormat)
		master.ReduceTasks[master.TaskRecord] = &reduceTaskInfo
		master.TaskRecord++

		reduceTaskInfo.DebugReduceBriefTaskInfo()
	}
}




func (master *Master) initPartitionMaps() {
	if master.Phase.Load() { panic("Trying to initialze the PartitonMaps when the Phase is true") }

	master.TaskMut.Lock()
	defer master.TaskMut.Unlock()

	for _, mapTasks := range master.MapTasks {
		for i, partitionFile := range mapTasks.IntermediatePaths {
			master.PartitionMaps[i] = append(master.PartitionMaps[i], partitionFile)
		}
	}
}


func (master *Master) waitEnoughWorker(ctx context.Context) {
	slog.Debug("Waiting for enough workers", "worker count", master.TotalWorkerCount)

	loop: 
	for {
		select {
		case <- ctx.Done():
			slog.Warn("The service of waiting enough workers is normally interrupted")
		default:
			if master.CurrentAvailableWorkerCount.Load() >= int32(master.TotalWorkerCount) {
				master.enoughWorker.Store(true)
				break loop  // break 可以退出 select， 即使 select 并不是循环
			}
		}
	}

	slog.Info("All workers is ready")
}


func (master *Master) rpcServiceCall(ctx context.Context) MasterError {
	// 1. 创建 gRPC 实例
	grpcServer := grpc.NewServer()

	// 2. 注册服务
	pb.RegisterMasterServiceServer(grpcServer, master)

	// 3. 指定端口上创建 TCP 监听器监听服务
	listen, err := net.Listen("tcp", ":50051")

	if err != nil {
		return NewMasterError(ServiceError, "create tcp service that use 50051 port error")
	}

	// 当 Serve 启动失败时，取消 终止Serve的goroutine
	goroutineCtx, goroutineCancel := context.WithCancel(context.Background())
	defer goroutineCancel()

	slog.Info("grpc service listen 50051 port")	

	go func() {
		select {
			case <- ctx.Done(): // 传入的 ctx 终止整个监听服务
			listen.Close()
			case <- goroutineCtx.Done(): // 里面创建的 ctx 应对 Serve 创建失败 goroutine 仍运行的问题
			return
		}
	}()

	err = grpcServer.Serve(listen)  // 内部调用 Accept() 阻塞代码

	if err != nil {
		if err == grpc.ErrServerStopped { // 正常退出
			slog.Info("grpc service stopped gracefully")
			return NewMasterNilError()
		}
		return NewMasterError(ServiceError, "grpc service listen 50051 port error")
	}

	return NewMasterNilError()
}


func (master *Master) existWorker(id string, generation int) (bool, MasterError) {
	master.WorkerMut.RLock()
	defer master.WorkerMut.RUnlock()

	workerInfo, exist := master.Workers[id]

	if !exist { return false, NewMasterNilError() } 

	if workerInfo.Generation != generation { return false, NewMasterError(WorkerNoSameGeneration, "worker %s has no same generation", id) }

	return true, NewMasterNilError()
}
