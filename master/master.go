package master

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
	"github.com/mapreduce_impl/utility"
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
	Phase TaskType
	MapTasks    map[int]*MapTaskInfo // 任务信息
	ReduceTasks map[int]*ReduceTaskInfo
	CurrentNumMapWorker    int // 执行 Map 任务的 Worker 数量
	CurrentNumReduceWorker int // 执行 Reduce 任务的 Worker 数量

	TaskRecord int   // 仅用于得到唯一的任务编号
	WorkerMut sync.Mutex
	MapTaskMut sync.Mutex
	ReduceTaskMut sync.Mutex
	PhaseMut sync.Mutex
	pb.UnimplementedMasterServiceServer
}


type TaskType int

const (
	WAIT TaskType = iota
	MAP
	REDUCE
) 

// 初始化固定信息
func NewMaster(address string) Master {
	return Master{
		ID:                      uuid.NewString(),
		Address:                   address,
		Workers: make(map[string]*WorkerInfo),
		CurrentNumWorker: 0,
		MapTasks: make(map[int]*MapTaskInfo),
		ReduceTasks: make(map[int]*ReduceTaskInfo),
		Phase: WAIT,
		TaskRecord: 0,
		WorkerMut: sync.Mutex{},
		MapTaskMut: sync.Mutex{},
		ReduceTaskMut: sync.Mutex{},
		PhaseMut: sync.Mutex{},
	}
}

// 初始化作用信息
func (master *Master) JobInitialzation() {
	master.TotalNumWorker = common.NumWorker
	master.InputFile = common.InputDir
	master.NumReduceTask = common.NReduce

	// 初始化 Map 作业
	master.InitMapJob()
	master.Phase = MAP
}

func (master *Master) InitMapJob() {
	// 计算 NumMapTask，Map 任务数量
	master.NumMapTask = utility.NuminputFile()
	// 初始化所有 Map 任务
	for range master.NumMapTask {
		inputFile := utility.GetinputFile()
		mapTaskFormat := NewMaptaskFormat(inputFile, master.IntermediateDir, master.NumReduceTask)
		mapTaskInfo := NewMapTaskInfo(master.TaskRecord, mapTaskFormat)
		master.MapTasks[master.TaskRecord] = &mapTaskInfo
		master.TaskRecord++
	}
}

// 启动 Master 服务
func (master *Master) StartService() {
	ctx := context.Background()	
	go func() {
		master.RpcServiceCall(ctx)
	}()
	master.WaitEnoughWorker(ctx)
}

func (master *Master) WaitEnoughWorker(ctx context.Context) {
	for {
		if master.CurrentNumWorker >= master.TotalNumWorker {
			break
		}
	}
	log.Printf("[system] We get enough workers\n")

}
// 注册服务：
// 如果是重新注册
// 重置其状态
// 如果首次注册
// 
func (master *Master) WorkerRegister(tx context.Context, req *pb.WorkerRegisterRequest) (*pb.WorkerRegisterReply, error) {
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if exist {
		// 检查 generation
		if workerInfo.Generation == int(req.Generation) {
			workerInfo.Generation++
			log.Printf("[worker register] %s", req.WorkerId) 
			return &pb.WorkerRegisterReply{
				Ok: true,
				MasterId: master.ID,
				Generation: int32(workerInfo.Generation),
			}, nil
		}
		// 世代号不同，注册失败
		log.Printf("[error] worker %s register fail, because the generation is no same", req.WorkerId)
		return &pb.WorkerRegisterReply{
			Ok: false,
			MasterId: master.ID,
			Generation: 0,
		}, nil
	}	
	// 不存在，注册
	NewWorkerInfo := NewWorkerInfo(req.WorkerId, 1, req.Address)
	master.WorkerMut.Lock()
	master.Workers[req.WorkerId] = &NewWorkerInfo
	master.CurrentNumWorker++
	master.WorkerMut.Unlock()
	log.Printf("[worker register] %s", req.WorkerId)
	return &pb.WorkerRegisterReply{
		Ok: true,
		MasterId: master.ID,
		Generation: 1,
	}, nil
}

func (master *Master) HeartbeatCheck(tx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	// 1. 获取 worker，如果没有，返回失败
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		return &pb.HeartbeatReply{Signal: pb.Signal_REGISTER}, nil
	}
	// 2. 判断世代号
	if workerInfo.Generation != int(req.Generation) {
		return &pb.HeartbeatReply{ Signal: pb.Signal_SHUTDOWN}, nil
	}
	// 3. 更新 LastPing
	workerInfo.LastPing = time.Now()
	return &pb.HeartbeatReply{Signal: pb.Signal_OK}, nil
}

func (master *Master) TaskApply(tx context.Context, req *pb.TaskApplyRequest) (*pb.TaskApplyReply, error) {
	// 1. 获取 worker， 判断世代号
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		return &pb.TaskApplyReply{ Signal: pb.Signal_REGISTER }, nil
	}
	if workerInfo.Generation != int(req.Generation) {
		return &pb.TaskApplyReply{ Signal: pb.Signal_SHUTDOWN }, nil
	}
	// 2. 分发任务
	master.PhaseMut.Lock()
	phase := master.Phase
	master.PhaseMut.Unlock()
	switch phase {
	case MAP: 
		master.MapTaskMut.Lock()
		var map_task *MapTaskInfo
		for _, taskInfo := range master.MapTasks {
			if taskInfo.Status == common.TaskStatusIdle {
				map_task = taskInfo
			}
		}
		map_task.Status = common.TaskStatusRunning
		map_task.WorkerID = req.WorkerId
		map_task.StartTime = time.Now()
		master.MapTaskMut.Unlock()

		workerInfo.RunningTask = map_task.ID

		task_info := pb.MapTaskInfo {
			TaskId: int32(map_task.ID),
			InputFile: map_task.MapTaskFormat.InputFile,
			NReduce: int32(map_task.MapTaskFormat.NReduce),
			IntermediateDir: map_task.MapTaskFormat.IntermediateDir,

		}
		log.Printf("[task distribute] task %d is distributed to worker %s", map_task.ID, req.WorkerId)
		return &pb.TaskApplyReply{Signal: pb.Signal_OK, TaskType: pb.TaskType_MAP, TaskInfo: &pb.TaskApplyReply_MapTaskInfo{MapTaskInfo: &task_info}}, nil
	case REDUCE:
		master.ReduceTaskMut.Lock()
		var reduce_task *ReduceTaskInfo
		for _, taskInfo := range master.ReduceTasks {
			if taskInfo.Status == common.TaskStatusIdle {
				reduce_task = taskInfo
			}
		}
		reduce_task.Status = common.TaskStatusRunning
		reduce_task.WorkerID = req.WorkerId
		reduce_task.StartTime = time.Now()
		master.ReduceTaskMut.Unlock()

		workerInfo.RunningTask = reduce_task.ID
	
		task_info := pb.ReduceTaskInfo {
			TaskId: int32(reduce_task.ID),
			PartitionIndex: int32(reduce_task.ReduceTaskFormat.PartitionIndex),
			PartitionPaths: reduce_task.InterMediateAddresses,
			OutputDir: reduce_task.OutputPath,
		}
		log.Printf("[task distribute] task %d is distributed to worker %s", reduce_task.ID, req.WorkerId)
		return &pb.TaskApplyReply{Signal: pb.Signal_OK, TaskType: pb.TaskType_REDUCE, TaskInfo: &pb.TaskApplyReply_ReduceTaskInfo{ReduceTaskInfo: &task_info}}, nil
	default:
		return &pb.TaskApplyReply{Signal: pb.Signal_SHUTDOWN}, nil
	}
}

// 在分发 reduce 任务之前，必须初始化 reduce 任务的输入文件路径
func InitReduceTask()

func (master *Master) TaskCompletion(ctx context.Context, req *pb.TaskCompletionRequest) (*pb.TaskCompletionReply, error) {
	// 1. 获取 worker 比较 generation
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		return &pb.TaskCompletionReply{Signal: pb.Signal_REGISTER}, nil	
	}
	if workerInfo.Generation != int(req.Generation) {
		return &pb.TaskCompletionReply{Signal: pb.Signal_SHUTDOWN}, nil
	}
	// 2. 获取 task，判断其状态是否合法
	switch req.TaskType {
	case pb.TaskType_MAP:
		master.MapTaskMut.Lock()
		task, exist := master.MapTasks[int(req.TaskId)]
		master.MapTaskMut.Unlock()
		if !exist {
			return &pb.TaskCompletionReply{Signal: pb.Signal_SHUTDOWN}, nil
		}
		// 更新 workerInfo，将 running -> completion
		workerInfo.RunningTask = -1
		workerInfo.CompletedTasks = append(workerInfo.CompletedTasks, int(req.TaskId))
		// 更新 taskInfo
		attach := req.Attach.(*pb.TaskCompletionRequest_MapAttach)
		intermediateFiles := attach.MapAttach.IntermediateFiles
		task.EndTime = time.Now()
		task.Status = common.TaskStatusCompletion
		task.InterMediateAddresses = intermediateFiles
		return &pb.TaskCompletionReply{Signal: pb.Signal_OK}, nil
	case pb.TaskType_REDUCE:
		master.ReduceTaskMut.Lock()
		task, exist := master.ReduceTasks[int(req.TaskId)]
		master.ReduceTaskMut.Unlock()
		if !exist {
			return &pb.TaskCompletionReply{ Signal: pb.Signal_SHUTDOWN}, nil
		}
		workerInfo.RunningTask = -1
		workerInfo.CompletedTasks = append(workerInfo.CompletedTasks, int(req.TaskId))
		attach := req.Attach.(*pb.TaskCompletionRequest_ReduceAttach)
		task.EndTime = time.Now()
		task.Status = common.TaskStatusCompletion
		task.OutputPath = attach.ReduceAttach.OutputFile
		return &pb.TaskCompletionReply{ Signal: pb.Signal_OK}, nil
	default:
		return &pb.TaskCompletionReply{Signal: pb.Signal_SHUTDOWN}, nil
	}
}
