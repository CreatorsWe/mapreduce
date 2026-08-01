package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
)




func (worker *Worker) registerWorkerService(ctx context.Context, cancel context.CancelFunc) bool {
	if worker.Status == common.WorkerStatusShutdown { 
		cancel()
		return false 
	}
	
	req := pb.WorkerRegisterRequest{
		WorkerId:    worker.ID,
		Address: worker.Address,
		Generation: int32(worker.Generation),
	}

	res, err := (*worker.service()).WorkerRegister(ctx, &req)

	if err != nil {
		slog.Error("worker register", "msg", err.Error())
		return false
	}

	switch res.Signal {
	case pb.Signal_OK:
		worker.updateWorkerGeneration(int(res.Generation))
		slog.Info("worker register", "msg", "worker register success")
		return true
	case pb.Signal_SHUTDOWN:
		worker.updateWorkerStatus(common.WorkerStatusShutdown)
		cancel()
	case pb.Signal_WAIT:
		time.Sleep(1 * time.Second)
	default:
		cancel()
		panic("unreachable case")
	}
	return false
}





