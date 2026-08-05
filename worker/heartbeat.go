package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
)

func (worker *Worker) heartbeatService(ctx context.Context, cancel context.CancelFunc) {
	if worker.Status == common.WorkerStatusShutdown {
		return
	}

	req := pb.HeartbeatRequest{
		WorkerId:   worker.ID,
		Generation: int32(worker.Generation),
	}

	res, err := (*worker.service()).Heartbeat(ctx, &req)

	if err != nil {
		slog.Error("heartbeat", "msg", err.Error())
		cancel()
	}

	switch res.Signal {
	case pb.Signal_OK:
		worker.updateLastPing(time.Now())
		slog.Debug("heartbeat", "msg", "heartbeat success")
	case pb.Signal_REGISTER:
		worker.RegisterUntilSucessOrShutdown(ctx, cancel)
	case pb.Signal_WAIT:
		time.Sleep(1 * time.Second)
	case pb.Signal_SHUTDOWN:
		worker.updateWorkerStatus(common.WorkerStatusShutdown)
		cancel()
	}
}
