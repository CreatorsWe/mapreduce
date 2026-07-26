package main

import (
	"fmt"

	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/flag"
	"github.com/mapreduce_impl/master"
)

func init() {
	flag.Parse()
}


func main() {
	master := master.NewMaster(common.MASTER_ADDRESS)
	master.JobInitialzation()
	master.StartService()
}
