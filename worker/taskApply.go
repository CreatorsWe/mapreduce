package worker

import (
	"log/slog"
	"time"
	"context"

	pb "github.com/mapreduce_impl/rpc"
	"github.com/mapreduce_impl/common"


)


// any 表示 Map 或 Reduce 任务执行需要的参数
func (worker *Worker) ApplyTaskService(ctx context.Context, cancel context.CancelFunc) any {
	if worker.Status == common.WorkerStatusShutdown { return nil }
	
	req := pb.TaskApplyRequest {
		WorkerId: worker.ID,
		Generation: int32(worker.Generation),
	}

	res, err := (*worker.service()).TaskApply(ctx, &req)

	if err != nil { panic(err.Error()) }
 
	switch res.Signal {
	case pb.Signal_OK:
		slog.Info("gain task", "task", res.TaskId)
		return getTaskInfo(res)
	case pb.Signal_REGISTER:
		worker.RegisterUntilSucessOrShutdown(ctx, cancel)
	case pb.Signal_SHUTDOWN:
		worker.updateWorkerStatus(common.WorkerStatusShutdown)
		cancel()
	case pb.Signal_WAIT:
		time.Sleep(1 * time.Second)
	}

	return nil
}


func getTaskInfo(res *pb.TaskApplyReply) any {
	switch res.TaskType {
	case pb.TaskType_MAP:
		resTaskInfo := res.GetMapTaskInfo()

		inputFormatter := common.NewInputFormatter(
			resTaskInfo.InputFormatter.InputPath, 
			int(resTaskInfo.InputFormatter.From), 
			int(resTaskInfo.InputFormatter.To),
		)

		mapTaskInfo := NewMapTaskInfo(
			int(res.TaskId), 
			inputFormatter, 
			int(resTaskInfo.PartitionCount), 
			resTaskInfo.IntermediateDir,
		)

		return mapTaskInfo
	case pb.TaskType_REDUCE:
		resTaskInfo := res.GetReduceTaskInfo()

		reduceTaskInfo := NewReduceTaskInfo(
			int(res.TaskId), 
			int(resTaskInfo.PartitionIndex), 
			resTaskInfo.PartitionPaths, 
			resTaskInfo.OutputDir,
		)
		return reduceTaskInfo
	default:
		panic("unreachable case")
	}
}
