from optparse import OptionParser
import commands
import os
from os import system
import socket

# # 8b5d20e
# # ./buy -nprocs 80 -ngo 80 -nw 80 -nsec 10 -contention 100000 -rr 90 -allocate=False -sys=1 -rlock=False -wr=3.0 -phase=80 -sr=10000 -retry=False -atomic=False -latency=False
# nworkers: 80
# nwmoved: 0
# nrmoved: 0
# sys: 1
# total/sec: 4.0391641537445895e+07
# abortrate: 9.72
# stashrate: 0.00
# rr: 90
# ncrr: 72
# nbids: 1000000
# nproducts: 10
# contention: 100000
# done: 406450603
# actual time: 10.062740397s
# nreads: 387004973
# nbuys: 19445630
# epoch changes: 0
# throughput ns/txn: 24
# naborts: 43750375
# coord time: 0
# coord stats time: 0
# total worker time transitioning: 0
# nstashed: 0
# rlock: false
# wrratio: 3
# nsamples: 0
# getkeys: 0
# ddwrites: 0
# nolock: 7183191
# failv: 0  
# txn0: 19445630
# txn2: 387004973
# chunk-mean: 0
# chunk-stddev: 0

# scale graphs: x-axis is nworkers, y-axis is total/sec.  sys +
# --retry gives different lines.  -rr gives different graphs; 10, 50,
# 90.

# rw graph: nworkers should be 40 on ben.  x-axis is -rr, y-axis is
# total/sec.  sys + --retry is different lines.

# single graph: binary is single, nworkers 40 on ben, x-axis is
# -contention, y-axis is total/sec, -sys+--retry gives different lines

# tom
# 1-18 or 18 cores
# max y-axis of 20M

# ben
# 1-80 or 40 cores
# max y-axis of 82M


parser = OptionParser()
parser.add_option("-g", "--graph", action="store", type="string", dest="graph", default="scale")
parser.add_option("-f", "--file", action="store", type="string", dest="fn", default="buy-data.out")

(options, args) = parser.parse_args()


def wrangle_file():
    pass

def make_graph(graph):
    pass

if __name__ == "__main__":
    pass
