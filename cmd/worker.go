package main

import "github.com/mapreduce_impl/worker"
import "fmt"
const WORKER_ADDRESS = "127.0.0.1:50052"

func main() {
	worker := worker.NewWorker(WORKER_ADDRESS)

	worker.Run()

	fmt.Printf("Worker Client Exit")

}
