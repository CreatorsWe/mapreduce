package worker

import (
	"cmp"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/mrapps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mapreduce_impl/rpc"
)

// map (k1, v1) -> List(k2, k2)
// reudce (k2, List(v2)) -> List(v3)

// map / reduce 是纯函数,不依赖外读状态,即使需要退出,也是由操作系统直接终止 worker 进程,而非退出 map/reduce 函数
type MapFunc func(string, string) common.ListKV

type ReduceFunc func(string, []string) common.KV

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

	masterConn MasterConn
}




type MasterConn struct {
	client *pb.MasterServiceClient
	conn *grpc.ClientConn
}




func NewWorker(address string) Worker {
	return Worker{
		ID:           uuid.NewString(),
		Address:        address,
		Status:         common.WorkerStatusIdle,
		LastPing:       time.Time{},
		RunningTask:   -1,
		CompletedTasks: nil,	
		Mapf: mrapps.Map,
		Reducef: mrapps.Reduce,
	}
}




func (worker *Worker) InitConn(master_address string) {
	// 客户端临时端口完全有操作系统分配，用户不应参与临时端口的分配
	conn, err := grpc.NewClient(common.MASTER_ADDRESS, grpc.WithTransportCredentials(insecure.NewCredentials())) 
	if err != nil {
		os.Exit(1)
	}


	client := pb.NewMasterServiceClient(conn)

	worker.masterConn.conn = conn 
	worker.masterConn.client = &client

}




func (worker *Worker) service() *pb.MasterServiceClient {
	return worker.masterConn.client
}





func (worker *Worker) Close() {
	worker.masterConn.conn.Close()
}




func (worker *Worker) Run() {
	defer worker.Close()
	// PRC 调用远程方法
	ctx, cancel := context.WithCancel(context.Background())

	worker.RegisterUntilSucessOrShutdown(ctx, cancel)	

	var wg sync.WaitGroup

	// hearbeat goroutine
	wg.Go(func() { worker.Heartbeat(ctx, cancel) })

	// 获取 -> 完成任务在同一个 goroutine 中运行
	wg.Go(func () { worker.ApplyAndCompleteTask(ctx, cancel) })

	wg.Wait()
}





func (worker *Worker) RegisterUntilSucessOrShutdown(ctx context.Context, cancel context.CancelFunc) {
	var isRegister bool = false

	for isRegister == false {
		select {
		case <- ctx.Done():
			return
		default:
			isRegister = worker.registerWorkerService(ctx, cancel)
		}
	}
}





func (worker *Worker) Heartbeat(ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
		case <- ctx.Done():
			return
		default:
			worker.heartbeatService(ctx, cancel)
		}
		// 阻塞 1s
		time.Sleep(1 * time.Second)
	}
}




func (worker *Worker) ApplyAndCompleteTask(ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
		case <- ctx.Done():
			return
		default:
			worker.applyAndCompleteTask(ctx, cancel)
		}
	}
}




func (worker *Worker) applyAndCompleteTask(ctx context.Context, cancel context.CancelFunc) {
	taskInfo := worker.ApplyTaskService(ctx, cancel)
	switch task_info := taskInfo.(type) {
	case MapTaskInfo:
		intermediatePaths := worker.ExecuteMapTask(task_info, ctx)

		worker.CompleteTaskService(ctx, cancel, task_info.taskID, pb.TaskType_MAP, intermediatePaths)
	case ReduceTaskInfo:
		outputFile := worker.ExecuteReduceTask(task_info, ctx)

		worker.CompleteTaskService(ctx, cancel, task_info.taskID, pb.TaskType_REDUCE, []string{outputFile})
	case nil:
		return
	}

}



// 更新 Worker 状态
func (worker *Worker) updateWorkerForTaskCompletion(task_id int) {
	worker.RunningTask = -1
	worker.CompletedTasks = append(worker.CompletedTasks, task_id)
	worker.Status = common.WorkerStatusIdle
}




func (worker *Worker) updateWorkerForTaskExucuting(task_id int) {
	worker.Status = common.WorkerStatusBusy
	worker.RunningTask = task_id
}



func (worker *Worker) updateWorkerStatus(status common.WorkerStatus) {
	worker.Status = status
}



func (worker *Worker) updateWorkerGeneration(generation int) {
	if generation != worker.Generation + 1 { 
		panic(fmt.Sprintf("worker generation: %d, to-update generation: %d", worker.Generation, generation)) 
	}
	worker.Generation++
}



func (worker *Worker) updateLastPing(last_ping time.Time) {
	worker.LastPing = last_ping
}




// 执行任务
func (worker *Worker) ExecuteMapTask(map_task_info MapTaskInfo, ctx context.Context) []string {
	slog.Info("execute task", "task", map_task_info.taskID)

	worker.updateWorkerForTaskExucuting(map_task_info.taskID)

	content := readInputFormatter(map_task_info.inputFormatter)

	if content == "" { 
		slog.Warn(
			"read no data from input formatter", 
			"input file", map_task_info.inputFormatter.FilePath,
			"from", map_task_info.inputFormatter.From,
			"size", map_task_info.inputFormatter.Size,
		) 
	}

	// 2. 调用 map 函数
	result := worker.Mapf(map_task_info.inputFormatter.FilePath, content)
	// 3. 排序
	slices.SortFunc(result, func (a, b common.KV) int{
		return cmp.Compare(a.Key, b.Key)
	})
	// 4. 将结果写入文件
	files := writeIntermediateFile(map_task_info.intermediateDir, map_task_info.partitionCount, result, map_task_info.taskID)	
	return files
}




func (worker *Worker) ExecuteReduceTask(reduce_task_info ReduceTaskInfo, ctx context.Context) string {
	slog.Info("execute task", "task", reduce_task_info.taskID)

	worker.updateWorkerForTaskExucuting(reduce_task_info.taskID)

	// 2. 汇总相同键的结果
	data := mergeKey(reduce_task_info.partitionPaths) 

	// 3. 调用 reduce 函数
	var result common.ListKV
	for key, values := range data {
		reduce := worker.Reducef(key, values)
		result = append(result,  reduce)
	}
	// 4. 将每个键最终结果写入文件
	fileName := fmt.Sprintf("part-%d", reduce_task_info.taskID)
	filePath := path.Join(reduce_task_info.outputDir, fileName)
	fileContent := result.ToJSONs()
	err := os.WriteFile(filePath, []byte(fileContent), 0644)
	if err != nil { 
		panic(fmt.Sprintf("write file %s error", filePath))
	}
	return filePath
}




func writeIntermediateFile(intermediate_dir string, partition_count int, list_kv common.ListKV, task_id int) []string {
	if list_kv.Len() == 0 { slog.Warn("key-value data is null") }

	files := make([]string, partition_count)

	// 1. 初始化文件列表
	for index := range partition_count {
		file_name := fmt.Sprintf("map-%d-output-%d", task_id, index)
		file_path := path.Join(intermediate_dir, file_name)
		files[index] = file_path
	}
	// 2. 分区写入
	data := make([][]string, partition_count)
	for _, kv := range list_kv {
		partition_index := int(hash32(kv.Key) % partition_count) 
		data[partition_index] = append(data[partition_index], kv.ToJSON()) 
	} 

	// 3. 写入文件
	for i := range partition_count {
		content := strings.Join(data[i], "\n")
		err := os.WriteFile(files[i], []byte(content), 0644)
		if err != nil { panic(fmt.Sprintf("write file %s error", files[i])) }
	}
	return files
}



func hash32(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32())
}



func readInputFormatter(input_formatter common.InputFormatter) string {
	file, err := os.Open(input_formatter.FilePath)
	if err != nil { panic(err) }
	defer file.Close()

	// 偏移量
	offset := int64(input_formatter.From)

	size := input_formatter.Size

	buf := make([]byte, size)

	_, err = file.ReadAt(buf, offset)


	if err != nil && err != io.EOF { panic(err) }

	return string(buf)
}



// 读取该分区的所有文件并合并相同的键
func mergeKey(partition_paths []string) map[string][]string {
	data := make(map[string][]string)

	for _, file := range partition_paths {
		content, err := os.ReadFile(file)
		if err != nil { panic(fmt.Sprintf("read intermediate file %s error", file)) }
		for line := range strings.SplitSeq(string(content), "\n") {
			if line == "" { continue }
			kv := common.FromJSON(line)
			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
	} 

	return data
}
