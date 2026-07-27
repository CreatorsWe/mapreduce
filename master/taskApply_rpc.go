package master


import (
	"context"
	"log"
	"time"
	pb "github.com/mapreduce_impl/rpc"
	"github.com/mapreduce_impl/common"
)



func (master *Master) TaskApply(tx context.Context, req *pb.TaskApplyRequest) (*pb.TaskApplyReply, error) {
	log.Printf("[task apply] worker %s reqeust task\n", req.WorkerId)
	// 1. 获取 worker， 判断世代号
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		log.Printf("[task apply] worker %s do not exist, response re-REGISTER signal\n", req.WorkerId)
		return &pb.TaskApplyReply{ Signal: pb.Signal_REGISTER }, nil
	}
	if workerInfo.Generation != int(req.Generation) {
		log.Printf("[task apply] worker %s have no same generation, reponse SHUTDOWN signal\n", req.WorkerId)
		return &pb.TaskApplyReply{ Signal: pb.Signal_SHUTDOWN }, nil
	}
	// 2. 分发任务
	phase := master.Phase.Load()
	switch phase {
	case true: // 分发 Map 任务 
		map_task := master.getIdleMapTask()
		if map_task == nil {
			if common.IsDebug {
				log.Printf("[debug] There is no Idle Map task but phase still is true\n")
			}
			return &pb.TaskApplyReply{Signal: pb.Signal_WAIT}, nil
		}
		map_task.Status = common.TaskStatusRunning
		map_task.WorkerID = req.WorkerId
		map_task.StartTime = time.Now()

		workerInfo.RunningTask = map_task.ID

		task_info := pb.MapTaskInfo {
			TaskId: int32(map_task.ID),
			InputFile: map_task.MapTaskFormat.InputFile,
			NReduce: int32(map_task.MapTaskFormat.NReduce),
			IntermediateDir: map_task.MapTaskFormat.IntermediateDir,

		}
		log.Printf("[task apply] task %d is distributed to worker %s", map_task.ID, req.WorkerId)
		return &pb.TaskApplyReply{Signal: pb.Signal_OK, TaskType: pb.TaskType_MAP, TaskInfo: &pb.TaskApplyReply_MapTaskInfo{MapTaskInfo: &task_info}}, nil
	case false:
		reduce_task := master.getIdleReduceTask()
		if reduce_task == nil {
			if common.IsDebug {
				log.Printf("[debug] There is no Idle Reduce task\n")
			}
			return &pb.TaskApplyReply{ Signal: pb.Signal_SHUTDOWN}, nil
		}
		reduce_task.Status = common.TaskStatusRunning
		reduce_task.WorkerID = req.WorkerId
		reduce_task.StartTime = time.Now()

		workerInfo.RunningTask = reduce_task.ID
	
		task_info := pb.ReduceTaskInfo {
			TaskId: int32(reduce_task.ID),
			PartitionIndex: int32(reduce_task.ReduceTaskFormat.PartitionIndex),
			PartitionPaths: reduce_task.InterMediateAddresses,
			OutputDir: reduce_task.OutputPath,
		}
		log.Printf("[task apply] task %d is distributed to worker %s", reduce_task.ID, req.WorkerId)
		return &pb.TaskApplyReply{Signal: pb.Signal_OK, TaskType: pb.TaskType_REDUCE, TaskInfo: &pb.TaskApplyReply_ReduceTaskInfo{ReduceTaskInfo: &task_info}}, nil
	default:
		return &pb.TaskApplyReply{Signal: pb.Signal_SHUTDOWN}, nil
	}
}
