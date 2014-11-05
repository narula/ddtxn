This is the code for Doppel, an in-memory key/value transactional
database.  WARNING: This is research code.  Use at your own risk.

Doppel's design is described in ["Phase Reconciliation for Contended
In-Memory Transactions"](http://pdos.csail.mit.edu/~neha/phaser.pdf),
presented at OSDI 2014.

To run Doppel, install go from source following the instructions here:<br>
`https://golang.org/doc/install/source`

Add the code in spin_loop.{1,2} to the appropriate parts of your go install:<br>
`cat spin_loop.1 >> $GOROOT/src/sync/atomic/doc.go`<br>
`cat spin_loop.2 >> $GOROOT/src/sync/atomic/asm_amd64.s`

Clone the Doppel code into your $GOPATH/src/ directory:<br>
`cd $GOPATH/src/`<br>
`git clone https://github.com/narula/ddtxn.git`

Run the tests:<br>
`cd ddtxn`<br>
`go test`

Run a benchmark:<br>
`cd ddtxn/benchmarks`<br>
`go build single.go`<br>
`python bm.py --exp=single --rlock --ncores=N`