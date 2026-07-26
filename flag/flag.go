package flag

import (
	"flag"

	"fmt"
	"github.com/mapreduce_impl/common"
)


func init() {
	fmt.Println("flag init execute")
	flag.IntVar(&common.NumWorker, "numWorker", 0, "the number of worker")
	flag.IntVar(&common.NReduce, "nReduce", 0, "the number of Reduce task")
	flag.StringVar(&common.InputDir, "inputPath", "", "the simulation of big file")
	flag.StringVar(&common.IntermediateDir, "intermediateDir", "", "the intermediate directory of the Map task generated")
	flag.StringVar(&common.OutputDir, "outputDir", "", "the finally ouput path of the Reduce task generated")
}

func Parse() {
	flag.Parse()
}

