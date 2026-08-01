package flag

import (
	"flag"

	"github.com/mapreduce_impl/common"
)


func init() {
	flag.IntVar(&common.WorkerCount, "w", 0, "the number of worker")
	flag.IntVar(&common.ReduceCount, "r", 0, "the number of Reduce task")
	flag.IntVar(&common.BlockSize, "b", 1024, "the size of bytes that a Map task handle")
	flag.StringVar(&common.IntermediateDir, "u", "", "the intermediate directory of the Map task generated")
	flag.StringVar(&common.OutputDir, "o", "", "the finally ouput path of the Reduce task generated")
	flag.IntVar(&common.Timeout, "t", 10, "the time-out duration(s) of the time-out check")
	flag.IntVar(&common.Periodic, "p", 15, "the period of the time-out check")
}

func Parse() {
	flag.Parse()

	inputFiles := flag.Args()

	if inputFiles == nil { panic("need input files") }

	common.InputFiles = inputFiles
}

