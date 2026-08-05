package master

import (
	"context"
	"log/slog"

	. "github.com/mapreduce_impl/common"
	pb "github.com/mapreduce_impl/rpc"
)

func (master *Master) WorkerRegister(tx context.Context, req *pb.WorkerRegisterRequest) (*pb.WorkerRegisterReply, error) {
	gen, err := master.RegisterWorker(req.WorkerId, int(req.Generation), req.Address)

	switch err.Code() {
	case WorkerNoSameGeneration:
		slog.Warn("worker register fail", "msg", err.String())
		return &pb.WorkerRegisterReply{
			Signal:     pb.Signal_SHUTDOWN,
			MasterId:   master.ID,
			Generation: 0,
		}, nil
	case Nil:
		slog.Info("worker register success", "worker_id", req.WorkerId, "generation", gen)
		return &pb.WorkerRegisterReply{
			Signal:     pb.Signal_OK,
			MasterId:   master.ID,
			Generation: int32(gen),
		}, nil
	default: // 代码层面的错误，日志必须是 Fatal 或者 panic，退出程序
		panic("unreachable case. Module: WorkerRegister")
	}
}
