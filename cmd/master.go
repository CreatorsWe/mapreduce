package main

import (
	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/master"
)

func main() {
	master := master.NewMaster(8, 10, 5, common.MASTER_ADDRESS, "/home/PatrickStar/Project/mapReduce_impl/input/file-01.txt")

	master.StartService()
}
