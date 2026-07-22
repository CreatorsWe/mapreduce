package flag

import (
	"flag"

	"github.com/mapreduce_impl/common"
)


func init() {
	flag.IntVar(&common.NumWorker, "numWorker", 0, "the number of worker")
	flag.IntVar(&common.NReduce, "nReduce", 0, "the number of Reduce task")
	flag.StringVar(&common.InputDir, "inputPath", "", "the simulation of big file")
	flag.StringVar(&common.IntermediateDir, "intermediateDir", "", "the intermediate directory of the Map task generated")
	flag.StringVar(&common.OutputDir, "outputDir", "", "the finally ouput path of the Reduce task generated")
}

func Parse() {
	flag.Parse()
}

