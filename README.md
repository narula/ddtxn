This is the code for Doppel, an in-memory key/value transactional
database.  WARNING: This is research code, and does not include
durability or RPC.  Use at your own risk.

Doppel's design is described in ["Phase Reconciliation for Contended
In-Memory Transactions"](http://pdos.csail.mit.edu/~neha/phaser.pdf),
presented at OSDI 2014.

If you don't care about the benchmarks in the paper, you can use a released version of go.  
To run the Doppel benchmarks, install go from source following the instructions here:

    https://golang.org/doc/install/source

Right now the only way to install ddtxn is to clone the repository.

Assuming the code will be at `$GOPATH/src/ddtxn`:

To run the tests, use `go test`.

Add `$GOPATH/bin` to your `PATH` environment variable.

Run a benchmark:

    cd $GOPATH/src/ddtxn/benchmarks
    go install ./single
    python bm.py --exp=single --rlock --ncores=N
