Install go following the instructions here:<br>
`https://golang.org/doc/install/source`

Create a directory hierarchy for go source code:<br>
`mkdir ~/gocode/src/`

set your GOPATH to the root of this directory:<br>
`GOPATH=~/gocode`

Clone the doppel code into the gocode/src/ directory:<br>
`cd ~/gocode/src/`
`git clone https://github.com/narula/ddtxn.git`

Run the tests:<br>
`cd ddtxn`<br>
`go test`

Run a benchmark:<br>
`cd ddtxn/benchmarks`<br>
`go build single.go`<br>
`python bm.py --exp=single --rlock --ncores=N`