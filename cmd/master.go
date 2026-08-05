package main

import (
	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/flag"
	"github.com/mapreduce_impl/master"
)

func main() {
	flag.Parse()
	master := master.NewMaster(common.MASTER_ADDRESS)
	master.StartService()
}
