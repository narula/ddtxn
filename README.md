This is the code for Doppel, an in-memory key/value transactional
database.  WARNING: This is research code, and does not include
durability or RPC.  Use at your own risk.

Doppel's design is described in ["Phase Reconciliation for Contended
In-Memory Transactions"](http://pdos.csail.mit.edu/~neha/phaser.pdf),
presented at OSDI 2014.

If you don't care about the benchmarks in the paper, you can use a released version of go.  
To run the Doppel benchmarks, install go from source following the instructions here:<br>
`https://golang.org/doc/install/source`

Clone the Doppel code into your $GOPATH/src/ directory:<br>
`cd $GOPATH/src/`<br>
`git clone https://github.com/narula/ddtxn.git`

Run the tests:<br>
`cd ddtxn`<br>
`go test`

Add bin/ to your PATH<br>

Run a benchmark:<br>
`cd ddtxn/benchmarks`<br>
`go build single.go`<br>
`python bm.py --exp=single --rlock --ncores=N`