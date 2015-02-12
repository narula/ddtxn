This is the code for Doppel, an in-memory key/value transactional
database.  WARNING: This is research code, and does not include
durability or RPC.  Use at your own risk.

Doppel's design is described in ["Phase Reconciliation for Contended
In-Memory Transactions"](http://pdos.csail.mit.edu/~neha/phaser.pdf),
presented at OSDI 2014.

Get the code:
    go get github.com/narula/dlog
    go get github.com/narula/prof
    go get github.com/narula/wfmutex
    go get github.com/narula/ddtxn
    
To run the tests, use `go test`.

Add `$GOPATH/bin` to your `PATH` environment variable.

Run a benchmark:

    cd $GOPATH/src/github.com/narula/ddtxn/benchmarks
    go install ./single
    python bm.py --exp=single --rlock --ncores=N
