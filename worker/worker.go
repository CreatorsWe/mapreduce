package worker

import (
	"context"
	"log"
	"os"
	"time"
	"github.com/google/uuid"
	"github.com/mapreduce_impl/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mapreduce_impl/rpc"
)

// map (k1, v1) -> List(k2, k2)
// reudce (k2, List(v2)) -> List(v3)

// map / reduce 是纯函数,不依赖外读状态,即使需要退出,也是由操作系统直接终止 worker 进程,而非退出 map/reduce 函数
type MapFunc func(string, string) ListKV

type ReduceFunc func(string, ListKV) ListKV

// 一个 Worker 两个 goroutine: 一个执行任务(简单), 一个发送心跳
type Worker struct {
	ID    string // unique identifier of the Worker.  use for communication
	Address string

	Generation int

	Mapf    MapFunc
	Reducef ReduceFunc

	Status  common.WorkerStatus

	LastPing time.Time // 超时检测

	// 执行的任务
	RunningTask   int
	CompletedTasks []int // 已完成的任务列表，如果该机器故障，Master 将重新分配所有已执行的任务
}

func NewWorker(address string) Worker {
	return Worker{
		ID:           uuid.NewString(),
		Address:        address,
		Mapf:           nil,
		Reducef:        nil,
		Status:         common.WorkerStatusIdle,
		LastPing:       time.Time{},
		RunningTask:   -1,
		CompletedTasks: nil,
	}
}

func (worker *Worker) Run() {
	// 客户端临时端口完全有操作系统分配，用户不应参与临时端口的分配
	conn, err := grpc.NewClient(common.MASTER_ADDRESS, grpc.WithTransportCredentials(insecure.NewCredentials())) 
	if err != nil {
		os.Exit(1)
	}

	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	// PRC 调用远程方法
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := pb.WorkerRegisterRequest{
		WorkerId:    worker.ID,
		Address: worker.Address,
		Generation: 0,
	}

	response, err := client.WorkerRegister(ctx, &req)
	if err != nil {
		log.Fatalf("[worker] %s worker register error", worker.ID)
	}

	if !response.Ok {
		log.Fatalf("[worker] %s worker register error", worker.ID)
	}
	worker.Generation = int(response.Generation)

	log.Printf("[worker] %s worker register success", worker.ID)

	// 加载插件 Mapf Reducef
}
