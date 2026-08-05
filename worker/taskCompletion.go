package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
)

func (worker *Worker) CompleteTaskService(ctx context.Context, cancel context.CancelFunc, task_id int, task_type pb.TaskType, files []string) {
	if files == nil {
		panic("files can not be nil")
	}
	if worker.Status == common.WorkerStatusShutdown {
		return
	}

	req := getCompletionRequest(worker.ID, worker.Generation, task_id, task_type, files)

	res, _ := (*worker.service()).TaskCompletion(ctx, req)

	switch res.Signal {
	case pb.Signal_OK:
		slog.Info("complete task", "msg", fmt.Sprintf("the task %d is completed", task_id))
		worker.updateWorkerForTaskCompletion(task_id)
	case pb.Signal_REGISTER:
		worker.RegisterUntilSucessOrShutdown(ctx, cancel)
	case pb.Signal_SHUTDOWN:
		worker.updateWorkerStatus(common.WorkerStatusShutdown)
		cancel()
	case pb.Signal_WAIT:
		time.Sleep(1 * time.Second)
	}
}

func getCompletionRequest(worker_id string, generation int, task_id int, task_type pb.TaskType, files []string) *pb.TaskCompletionRequest {
	var req pb.TaskCompletionRequest

	switch task_type {
	case pb.TaskType_MAP:
		req = pb.TaskCompletionRequest{
			WorkerId:   worker_id,
			Generation: int32(generation),
			TaskId:     int32(task_id),
			TaskType:   task_type,
			Attach:     &pb.TaskCompletionRequest_MapAttach{MapAttach: &pb.CompletionMapAttach{IntermediatePaths: files}},
		}
	case pb.TaskType_REDUCE:
		req = pb.TaskCompletionRequest{
			WorkerId:   worker_id,
			Generation: int32(generation),
			TaskId:     int32(task_id),
			TaskType:   task_type,
			Attach:     &pb.TaskCompletionRequest_ReduceAttach{ReduceAttach: &pb.CompletionReduceAttach{OutputPath: files[0]}},
		}
	}

	return &req
}
