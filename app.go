package ddtxn

type App interface {
	DoOne(*Worker, *uint32) *Result
	MakeOne(*Worker, *uint32) *Query
}
