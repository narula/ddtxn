1.  Install go following the instructions here:

https://golang.org/doc/install/source

2.  Create a directory hierarchy for go source code:

mkdir ~/gocode/src/

3.  set your GOPATH to the root of this directory:

GOPATH=~/gocode

4.  Clone the doppel code into the gocode/src/ directory:

cd ~/gocode/src/
git clone https://github.com/narula/ddtxn.git

5.  Run the tests:

cd ddtxn
go test

6.  Run a benchmark:

cd ddtxn/benchmarks
go build single.go
python bm.py --exp=single --rlock --ncores=N