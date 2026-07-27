package master
import (
	"context"
	"time"
	"log"
	pb "github.com/mapreduce_impl/rpc"

)

func (master *Master) HeartbeatCheck(tx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatReply, error) {
	// 1. 获取 worker，如果没有，返回失败
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if !exist {
		return &pb.HeartbeatReply{Signal: pb.Signal_REGISTER}, nil
	}
	// 2. 判断世代号
	if workerInfo.Generation != int(req.Generation) {
		return &pb.HeartbeatReply{ Signal: pb.Signal_SHUTDOWN}, nil
	}
	// 3. 更新 LastPing
	workerInfo.LastPing = time.Now()
	log.Printf("[heartbeat] worker %s update last time\n")
	return &pb.HeartbeatReply{Signal: pb.Signal_OK}, nil
}
