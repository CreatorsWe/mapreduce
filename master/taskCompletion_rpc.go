package master


import (
	"context"
	"log"
	"time"
	pb "github.com/mapreduce_impl/rpc"
	"github.com/mapreduce_impl/common"
)


func (master *Master) TaskCompletion(ctx context.Context, req *pb.TaskCompletionRequest) (*pb.TaskCompletionReply, error) {
	log.Printf("[task completion] worker %s complete %s task %d\n", req.WorkerId, req.TaskType, req.TaskId)
	// 1. 获取 worker 比较 generation
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		log.Printf("[task completion] worker %s do not exist\n")
		return &pb.TaskCompletionReply{Signal: pb.Signal_REGISTER}, nil	
	}
	if workerInfo.Generation != int(req.Generation) {
		log.Printf("[task completion] worker %s has no same generation\n")
		return &pb.TaskCompletionReply{Signal: pb.Signal_SHUTDOWN}, nil
	}
	// 2. 获取 task，判断其状态是否合法
	switch req.TaskType {
	case pb.TaskType_MAP:
		master.TaskMut.Lock()
		task, exist := master.MapTasks[int(req.TaskId)]
		master.TaskMut.Unlock()
		if !exist {
			log.Printf("[task completion] task %d do not exist\n", req.TaskId)
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

		if master.isMapTasksCompletion() {
			master.Phase.Store(false)
		}

		return &pb.TaskCompletionReply{Signal: pb.Signal_OK}, nil
	case pb.TaskType_REDUCE:
		master.TaskMut.Lock()
		task, exist := master.ReduceTasks[int(req.TaskId)]
		master.TaskMut.Unlock()
		if !exist {
			log.Printf("[task completion] task %d do not exist\n", req.TaskId)
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

