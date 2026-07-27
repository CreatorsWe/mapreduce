package master

import (
	"context"
	"log"
	pb "github.com/mapreduce_impl/rpc"
)



func (master *Master) WorkerRegister(tx context.Context, req *pb.WorkerRegisterRequest) (*pb.WorkerRegisterReply, error) {
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[req.WorkerId]
	master.WorkerMut.Unlock()
	if exist {
		// 检查 generation
		if workerInfo.Generation == int(req.Generation) {
			workerInfo.Generation++
			log.Printf("[worker register] %s", req.WorkerId) 
			return &pb.WorkerRegisterReply{
				Ok: true,
				MasterId: master.ID,
				Generation: int32(workerInfo.Generation),
			}, nil
		}
		// 世代号不同，注册失败
		log.Printf("[worker register] worker %s register fail, because the generation is no same", req.WorkerId)
		return &pb.WorkerRegisterReply{
			Ok: false,
			MasterId: master.ID,
			Generation: 0,
		}, nil
	}	
	// 不存在，注册
	NewWorkerInfo := NewWorkerInfo(req.WorkerId, 1, req.Address)
	master.WorkerMut.Lock()
	master.Workers[req.WorkerId] = &NewWorkerInfo
	master.CurrentNumWorker++
	master.WorkerMut.Unlock()
	log.Printf("[worker register] %s", req.WorkerId)
	return &pb.WorkerRegisterReply{
		Ok: true,
		MasterId: master.ID,
		Generation: 1,
	}, nil
}
