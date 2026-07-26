package utility

import (
	"testing"

	"github.com/mapreduce_impl/flag"
	"github.com/mapreduce_impl/common"
)


func TestParseInputDir(t *testing.T) {
	flag.Parse()

	t.Logf("%s", common.InputDir)
	for {
		file, err := ParseInputDir()
		if err != nil { 
			t.Errorf("%s", err)
			break
		}
		t.Logf("file: %s", file)
	}

}
