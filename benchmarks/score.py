from optparse import OptionParser
import commands
import os
from os import system
import socket

parser = OptionParser()
parser.add_option("-s", "--short", action="store_true", dest="short", default=False)
parser.add_option("-p", "--print", action="store_true", dest="dprint", default=False)
parser.add_option("-n", "--ncores", action="store", type="int", dest="default_ncores", default=8)
parser.add_option("-c", "--contention", action="store", type="int", dest="default_contention", default=100000)
parser.add_option("-r", "--rr", action="store", type="int", dest="read_rate", default=50)
parser.add_option("-m", "--scp", action="store_true", dest="scp", default=False)

(options, args) = parser.parse_args()

ben_list_cpus = "socket@0,1,2,7,3-6"

BASE_CMD = "GOGC=500 numactl -C `list-cpus seq -n %d %s` ./buy -nprocs %d -nsec %d -contention %d -rr %d -allocate=false -sys=%d -rlock=false -latency=false -wr=%d"

def run_one(fn, cmd):
    if options.dprint:
        print cmd
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Bad status", status, output
        exit(1)
    if options.dprint:
        print output
    fields = output.split(",")
    x = 0
    for f in fields:
        if "total/sec" in f:
            x = f.split(":")[1]
    tps = float(x)
    fn.write("%0.2f\t" % tps)

def get_cpus(host):
    ncpus = [1, 2, 4, 8]
    if host == "mat":
        ncpus = [1, 2, 4, 8, 12, 24]
    elif host == "tbilisi":
        ncpus = [1, 2, 4, 8, 12]
    elif host == "tom":
        ncpus = [1, 2, 6, 12, 18, 24, 30, 42, 48]
    elif host == "ben":
        ncpus = [1, 4, 10, 20, 30, 40, 50, 60, 70, 80]
    if options.short:
        ncpus=[2, 4]
    return ncpus

def fill_cmd(rr, contention, ncpus, systype, cpus_arg, ratio):
    nsec = 10
    if options.short:
        nsec = 1
    cmd = BASE_CMD % (ncpus, cpus_arg, ncpus, nsec, contention, rr, systype, ratio)
    return cmd

def do(f, rr, contention, ncpu, list_cpus, sys, ratio):
    cmd = fill_cmd(rr, contention, ncpu, sys, list_cpus, ratio)
    run_one(f, cmd)
    f.write("\t")

# x-axis is # cores
def contention_exp(fnpath, host, contention, rr):
    fnn = '%s-tune-%d-%d.data' % (host, contention, rr)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#OCC\tDoppel1K\tDoppel1\tDoppel2\tDoppel3\tDoppel5\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, contention, i, cpu_args, 1, 0)
        f.write("\t")
        for ratio in [1000, 1, 2, 3, 5]:
            do(f, rr, contention, i, cpu_args, 0, ratio)
            f.write("\t")
        f.write("\n")
    f.close()
    system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
    system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

if __name__ == "__main__":
    host = socket.gethostname()
    if len(host.split(".")) > 1:
        host = host.split(".")[0]
    fnpath = 'tmp/'
    if not os.path.exists(fnpath):
        os.mkdir(fnpath)
    contention_exp(fnpath, host, options.default_contention, options.read_rate)
