package apps

import (
	"ddtxn"
	"time"
)

type App interface {
	Init(s *ddtxn.Store, np, nb, nw, rr, ngo int, ncrr float64, ex *ddtxn.ETransaction)
	SetupLatency(int64, int64, int)
	MakeOne(int, *uint32, *ddtxn.Query)
	Add(ddtxn.Query)
	Validate(*ddtxn.Store, int) bool
	Time(*ddtxn.Query, time.Duration, int)
	LatencyString(int) (string, string)
}
