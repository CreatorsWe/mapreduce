package master

import (
	"fmt"
	"os"

	"github.com/mapreduce_impl/common"
)



// 检查所有 Map 任务是否都执行完成
// 检查所有 Map 状态是否都是 Completion
func (master *Master) IsMapTasksCompletion() bool {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	for _, mt := range master.MapTasks {
		if mt.Status != common.TaskStatusCompletion {
			return false
		}	
	}
	return true
}


func (master *Master) IsAllTaskCompletion() bool {
	master.TaskMut.RLock()
	defer master.TaskMut.RUnlock()

	for _, mt := range master.MapTasks {
		if mt.Status != common.TaskStatusCompletion {
			return false
		}	
	}

	for _, rt := range master.ReduceTasks {
		if rt.Status != common.TaskStatusCompletion {
			return false
		}
	}
	
	return true
}


// 将 iputFiles 划分成 Map 任务
// Map_quantity（大概） = InputFileSize / BlockSize
// 统计所有输入文件的总大小，Block 大小作为输入参数
// 各个文件的大小 / BlockSize = 处理该文件需要的 Map 数量，初始化对应的 InputFormatter
func DivideToInputFormatters(input_files []string, block_size int) ([]common.InputFormatter, error) {
	var inputFormatters []common.InputFormatter
	for _, file := range input_files {
		inputFormatter, err := divideFileToInputFormatter(file, block_size)
		if err != nil {
			return nil, err
		} 
		inputFormatters = append(inputFormatters, inputFormatter...)
	}
	return inputFormatters, nil
}


func divideFileToInputFormatter(input_file string, block_size int) ([]common.InputFormatter, error) {
	size, err := getFileSize(input_file)

	if err != nil { return nil, err }

	if size == 0 { return nil, nil }

	numInputFormatter := ceilDiv(size, block_size)
	
	inputFormatters := make([]common.InputFormatter, 0, numInputFormatter)

	start_pos := 0

	for range (numInputFormatter - 1) {
		formatter := common.NewInputFormatter(input_file, start_pos, block_size)
		inputFormatters = append(inputFormatters, formatter)
		start_pos += block_size
	}

	// 最后一个 formatter end 特殊标识表示读取到文件末尾
	formatter := common.NewInputFormatter(input_file, start_pos, size - start_pos)
	inputFormatters = append(inputFormatters, formatter)

	return inputFormatters, nil
}



func getFileSize(input_file string) (int, error) {
	stat, err := os.Stat(input_file)

	if os.IsNotExist(err) {
		return -1, fmt.Errorf("%s do not exist", input_file)		
	}

	if err != nil {
		return -1, err
	}

	return int(stat.Size()), nil
} 

// 除法结果向上取整
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}
