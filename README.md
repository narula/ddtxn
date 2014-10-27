Install go following the instructions here:
https://golang.org/doc/install/source

Create a directory hierarchy for go source code:
mkdir ~/gocode/src/

set your GOPATH to the root of this directory:
GOPATH=~/gocode

Clone the doppel code into the gocode/src/ directory:
cd ~/gocode/src/
git clone https://github.com/narula/ddtxn.git

Run the tests:
cd ddtxn
go test

Run a benchmark:
cd ddtxn/benchmarks
go build single.go
python bm.py --exp=single --rlock --ncores=N