package master

import (
	"context"
	"log/slog"

	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
)



func (master *Master) TaskApply(tx context.Context, req *pb.TaskApplyRequest) (*pb.TaskApplyReply, error) {
	if !master.enoughWorker.Load() { 
		return &pb.TaskApplyReply{Signal: pb.Signal_WAIT}, nil
	}

	// 检查 Worker 是否合法
	err := master.CheckWorker(req.WorkerId, int(req.Generation))

	if !err.IsNil() { return &pb.TaskApplyReply{ Signal: pb.Signal_WAIT }, nil } 

	// 分发任务
	phase := master.Phase.Load()
	switch phase {
	case true: // 分发 Map 任务 
		task_id, exist := master.GetIdleMapTaskId()
		if !exist {
			return &pb.TaskApplyReply{ Signal: pb.Signal_WAIT }, nil
		}
		
		master.UpdateMapTaskForDivice(task_id, req.WorkerId)

		master.UpdateWorkerRunningTask(req.WorkerId, task_id)

		master.UpdateWorkerStatus(req.WorkerId, common.WorkerStatusBusy)

		pbMapTaskInfo := master.GetPbMapTaskInfoForDivice(task_id)

		slog.Info("divice Map task", "task", task_id, "worker", req.WorkerId)

		return &pb.TaskApplyReply{
			Signal: pb.Signal_OK, 
			TaskId: int32(task_id),
			TaskType: pb.TaskType_MAP, 
			TaskInfo: &pb.TaskApplyReply_MapTaskInfo{ MapTaskInfo: pbMapTaskInfo },
		}, nil
	case false:  // 需要初始化 Reduce 任务的 intermediate_files 列表
		task_id, exist := master.GetIdleReduceTaskId()
		if !exist {
			return &pb.TaskApplyReply{ Signal: pb.Signal_WAIT }, nil
		}

		master.UpdateReduceTaskForDivice(task_id, req.WorkerId)

		master.UpdateWorkerRunningTask(req.WorkerId, task_id)

		master.UpdateWorkerStatus(req.WorkerId, common.WorkerStatusBusy)

		pbReduceTaskInfo := master.GetPbReduceTaskInfoForDivice(task_id)

		slog.Info("divice Reduce task", "task", task_id, "worker", req.WorkerId)

		return &pb.TaskApplyReply{
			Signal: pb.Signal_OK, 
			TaskId: int32(task_id),
			TaskType: pb.TaskType_REDUCE, 
			TaskInfo: &pb.TaskApplyReply_ReduceTaskInfo{ ReduceTaskInfo: pbReduceTaskInfo },
		}, nil
	default:
		panic("unreachable case")
	}
}
