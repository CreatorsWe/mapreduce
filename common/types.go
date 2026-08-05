package common

import (
	"fmt"
)

type WorkerStatus int

const (
	WorkerStatusIdle     WorkerStatus = iota // 空闲，未执行任何任务
	WorkerStatusBusy                         // 繁忙，正在执行任务
	WorkerStatusDead                         // 已失效（心跳超时或主动退出）
	WorkerStatusShutdown                     // 已关闭
)

type TaskStatus int

const (
	TaskStatusIdle TaskStatus = iota
	TaskStatusRunning
	TaskStatusCompletion
	TaskStatusFatal
)

type MasterErrorCode int

const (
	ServiceError MasterErrorCode = iota
	WorkerNotExist
	WorkerNoSameGeneration
	WorkerDead
	WorkerShutdown
	WorkerStatusError
	TaskNotExist
	TaskError
	InitJobError
	Nil // nil 值
)

type MasterError struct {
	code MasterErrorCode // 错误吗区分错误类型，而不是错误信息，因为错误信息可能需要别的参数
	msg  string
}

func NewMasterError(code MasterErrorCode, msg string, a ...any) MasterError {
	return MasterError{
		code: code,
		msg:  fmt.Sprintf(msg, a...),
	}
}

func NewMasterNilError() MasterError {
	return MasterError{
		code: Nil,
		msg:  "",
	}
}

func (me *MasterError) IsNil() bool { return me.code == Nil }

func (me *MasterError) String() string { return me.msg }

func (me *MasterError) Code() MasterErrorCode { return me.code }
