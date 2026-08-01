package master


import (
	"context"
	"log/slog"
	pb "github.com/mapreduce_impl/rpc"
	. "github.com/mapreduce_impl/common"
)


func (master *Master) TaskCompletion(ctx context.Context, req *pb.TaskCompletionRequest) (*pb.TaskCompletionReply, error) {
	if !master.enoughWorker.Load() { 
		return &pb.TaskCompletionReply{ Signal: pb.Signal_WAIT }, nil
	}

	err := master.CheckWorker(req.WorkerId, int(req.Generation))

	if !err.IsNil() { return &pb.TaskCompletionReply{ Signal: pb.Signal_WAIT }, nil }

	switch req.TaskType {
	case pb.TaskType_MAP:
		// 更新 workerInfo
		master.UpdateWorkerRunningTask(req.WorkerId, -1)
		master.AppendWorkerComletionTask(req.WorkerId, int(req.TaskId))
		master.UpdateWorkerStatus(req.WorkerId, WorkerStatusIdle)
		// 更新 taskInfo
		attach := req.Attach.(*pb.TaskCompletionRequest_MapAttach)
		intermediatePaths := attach.MapAttach.IntermediatePaths
		master.UpdateMapTaskForCompletion(int(req.TaskId), req.WorkerId, intermediatePaths)
		
		slog.Info(
			"complete Map task", 
			"worker", req.WorkerId,
			"task", req.TaskId,
			"intermediate_paths", intermediatePaths,
		)

		if master.IsMapTasksCompletion() {
			master.Phase.Store(false)
			// 初始化 Reduce 任务
			master.InitReduceJob()
		}

		return &pb.TaskCompletionReply{ Signal: pb.Signal_OK }, nil
	case pb.TaskType_REDUCE:
		// 更新 workerInfo
		master.UpdateWorkerRunningTask(req.WorkerId, -1)
		master.AppendWorkerComletionTask(req.WorkerId, int(req.TaskId))
		master.UpdateWorkerStatus(req.WorkerId, WorkerStatusIdle)
		// 更新 taskInfo
		attach := req.Attach.(*pb.TaskCompletionRequest_ReduceAttach)
		outputPath := attach.ReduceAttach.OutputPath
		master.UpdateReduceTaskForCompletion(int(req.TaskId), req.WorkerId, outputPath)

		slog.Info(
			"complete Reduce task", 
			"worker", req.WorkerId,
			"task", req.TaskId,
			"output_path", outputPath,
		)

		// 所有任务完成，关闭所有 worker 状态
		if master.IsAllTaskCompletion() {
			slog.Info("all task completed")
			master.ShutdownAllWorker()
			return &pb.TaskCompletionReply{ Signal: pb.Signal_SHUTDOWN }, nil
		}

		return &pb.TaskCompletionReply{ Signal: pb.Signal_OK }, nil
	default:
		panic("unreachable case")
	}
}

