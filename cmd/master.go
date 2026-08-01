package main

import (
	"github.com/mapreduce_impl/common"
	"github.com/mapreduce_impl/master"
	"github.com/mapreduce_impl/flag"
)



func main() {
	flag.Parse()
	master := master.NewMaster(common.MASTER_ADDRESS)
	master.StartService()
}
