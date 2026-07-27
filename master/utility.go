package master

import "github.com/mapreduce_impl/common"
import "fmt"

// bug: getIdleMapTask 没有修改内部状态，同时调用可能返回同一个空闲任务
func (master *Master) getIdleMapTask() *MapTaskInfo {
	var mapTaskInfo *MapTaskInfo
	master.TaskMut.Lock()
	for _, mt := range master.MapTasks {
		if mt.Status == common.TaskStatusIdle {
			mapTaskInfo = mt
			break
		}
	}
	master.TaskMut.Unlock()
	return mapTaskInfo
}


func (master *Master) getIdleReduceTask() *ReduceTaskInfo {
	var reduceTaskInfo *ReduceTaskInfo
	master.TaskMut.Lock()
	for _, rt := range master.ReduceTasks {
		if rt.Status == common.TaskStatusIdle {
			reduceTaskInfo = rt
			break
		}
	}
	master.TaskMut.Unlock()
	return reduceTaskInfo
}


// 检查所有 Map 任务是否都执行完成
// 检查所有 Map 状态是否都是 Completion
func (master *Master) isMapTasksCompletion() bool {
	result := true
	master.TaskMut.Lock()
	for _, mt := range master.MapTasks {
		if mt.Status != common.TaskStatusCompletion {
			result = false
			break
		}	
	}
	return result
}


func (master *Master) getWorkerInfo(id string, generation int) (*WorkerInfo, string) {
	master.WorkerMut.Lock()
	workerInfo, exist := master.Workers[id]
	master.WorkerMut.Unlock()
	if !exist {
		return nil, fmt.Sprintf("worker %s do not eixst")
	}
	if workerInfo.Generation != generation {
		return nil, fmt.Sprintf("worker %s has no same generation")
	}
	return workerInfo, ""
} 
