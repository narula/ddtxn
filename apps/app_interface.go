package apps

import (
	"ddtxn"
	"flag"
	"time"
)

var Latency = flag.Bool("latency", false, "Measure latency")

type App interface {
	Init(np, nb, nw, rr, ngo int, ncrr float64)
	Populate(s *ddtxn.Store, ex *ddtxn.ETransaction)
	SetupLatency(int64, int64, int)
	MakeOne(int, *uint32, *ddtxn.Query)
	Add(ddtxn.Query)
	Validate(*ddtxn.Store, int) bool
	Time(*ddtxn.Query, time.Duration, int)
	LatencyString() (string, string)
}
