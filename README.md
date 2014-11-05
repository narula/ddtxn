Install go following the instructions here:<br>
`https://golang.org/doc/install/source`

Add the code in spin_loop.{1,2} to the appropriate parts of your go install:<br>
`cat spin_loop.1 >> $GOROOT/src/sync/atomic/doc.go`<br>
`cat spin_loop.2 >> $GOROOT/src/sync/atomic/asm_amd64.s`

Create a directory hierarchy for go source code:<br>
`mkdir ~/gocode/src/`

set your GOPATH to the root of this directory:<br>
`GOPATH=~/gocode`

Clone the doppel code into the gocode/src/ directory:<br>
`cd ~/gocode/src/`<br>
`git clone https://github.com/narula/ddtxn.git`

Run the tests:<br>
`cd ddtxn`<br>
`go test`

Run a benchmark:<br>
`cd ddtxn/benchmarks`<br>
`go build single.go`<br>
`python bm.py --exp=single --rlock --ncores=N`