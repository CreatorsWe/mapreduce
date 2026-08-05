package common

const MASTER_ADDRESS = "127.0.0.1:50051"

// 参数
var (
	WorkerCount     int
	ReduceCount     int
	BlockSize       int
	IntermediateDir string
	OutputDir       string
	InputFiles      []string
	Timeout         int
	Periodic        int
)
