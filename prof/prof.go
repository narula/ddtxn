package prof

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var blockprofile = flag.String("blockprofile", "", "write block profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

type Profile struct {
	bpf *os.File
}

func StartProfile() *Profile {
	p := &Profile{}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
	if *blockprofile != "" {
		var err error
		p.bpf, err = os.Create(*blockprofile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
	}
	return p
}

func (p *Profile) Stop() {
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	if *blockprofile != "" {
		pprof.Lookup("block").WriteTo(p.bpf, 0)
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
