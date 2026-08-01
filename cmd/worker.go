package main

import (

	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/worker"
)
const WORKER_ADDRESS = "127.0.0.1:50052"

func main() {
	worker := worker.NewWorker(WORKER_ADDRESS)
	worker.InitConn(common.MASTER_ADDRESS)
	worker.Run()


}
