package master
import (
	"context"
	"time"
	"log/slog"
	"fmt"
	
	. "github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"

)

// 处理错误状况
func (master *Master) Heartbeat(tx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	if !master.enoughWorker.Load() {
		slog.Info("heartbeat wait", "msg", fmt.Sprintf("recive worker %s heartbeat packet, but need to wait for a moment", req.WorkerId))
		return &pb.HeartbeatReply{ Signal: pb.Signal_WAIT }, nil
	}

	err := master.CheckWorker(req.WorkerId, int(req.Generation))

	switch err.Code() {
	case WorkerNotExist:
		slog.Warn("heartbeat fail", "msg", err.String())
		return &pb.HeartbeatReply{ Signal: pb.Signal_REGISTER }, nil
	case WorkerNoSameGeneration:
		slog.Error("heartbeat fail", "msg", err.String())
		// 可用的 worker 数量 -1
		master.CurrentAvailableWorkerCount.Add(-1)
		// 重置 Worker 执行的所有 Task 的状态
		master.ResetTasksOfWorker(req.WorkerId)
		// 重置 Worker 状态
		master.UpdateWorkerStatus(req.WorkerId, WorkerStatusShutdown)

		return &pb.HeartbeatReply{ Signal: pb.Signal_SHUTDOWN }, nil
	case WorkerShutdown:  // 表明该 Worker 已重置状态, 无需再次重置
		slog.Error("heartbeat fail", "msg", err.String())
		master.CurrentAvailableWorkerCount.Add(-1)
		return &pb.HeartbeatReply{
			Signal: pb.Signal_SHUTDOWN,
		}, nil
	case WorkerDead:
		// 重置 Worker 执行的所有 Task 的状态
		master.ResetTasksOfWorker(req.WorkerId)

		master.CurrentAvailableWorkerCount.Add(-1)
		slog.Warn("heartbeat fail", "msg", err.String())
		
		return &pb.HeartbeatReply{ Signal: pb.Signal_REGISTER }, nil
	case Nil:
		slog.Info("heartbeat success", "worker", req.WorkerId)
		master.UpdateWorkerLastPing(req.WorkerId, time.Now())

		return &pb.HeartbeatReply{ Signal: pb.Signal_OK }, nil
	default:  // 代码层面的错误，日志必须是 Fatal 或者 panic，退出程序
		panic("unreachable case. Module: WorkerRegister")
	}
}
