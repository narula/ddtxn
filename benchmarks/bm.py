from optparse import OptionParser
import commands
import os
from os import system
import socket

parser = OptionParser()
parser.add_option("-e", "--exp", action="store", type="string", dest="exp", default="contention")
parser.add_option("-s", "--short", action="store_true", dest="short", default=False)
parser.add_option("-p", "--print", action="store_true", dest="dprint", default=False)
parser.add_option("-a", "--allocate", action="store_true", dest="allocate", default=False)
parser.add_option("-n", "--ncores", action="store", type="int", dest="default_ncores", default=8)
parser.add_option("-c", "--contention", action="store", type="int", dest="default_contention", default=100000)
parser.add_option("-r", "--rr", action="store", type="int", dest="read_rate", default=50)
parser.add_option("-l", "--latency", action="store_true", dest="latency", default=False)
parser.add_option("-x", "--rlock", action="store_false", dest="rlock", default=True)
parser.add_option("-m", "--scp", action="store_true", dest="scp", default=False)

(options, args) = parser.parse_args()

ben_list_cpus = "socket@0,1,2,7,3-6"

LATENCY_PART = " -latency=%s" % options.latency

BASE_CMD = "GOGC=500 numactl -C `list-cpus seq -n %d %s` ./buy -ngo %d -nprocs %d -nsec %d -contention %d -rr %d -allocate=%s -sys=%d -rlock=%s" + LATENCY_PART

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

def fill_cmd(rr, contention, ncpus, systype, cpus_arg=""):
    nsec = 10
    if options.short:
        nsec = 1
    cmd = BASE_CMD % (ncpus, cpus_arg, ncpus, ncpus, nsec, contention, rr, options.allocate, systype, options.rlock)
    return cmd

def do(f, rr, contention, ncpu, list_cpus, sys):
    cmd = fill_cmd(rr, contention, ncpu, sys, list_cpus)
    run_one(f, cmd)
    f.write("\t")

# x-axis is # cores
def contention_exp(fnpath, host, contention, rr):
    fnn = '%s-scalability-%d-%d-False.data' % (host, contention, rr)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, contention, i, cpu_args, 0)
        do(f, rr, contention, i, cpu_args, 1)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)


def rw_exp(fnpath, host, contention, ncores):
    fnn = '%s-rw-%d-%d-False.data' % (host, contention, ncores)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50, 100]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    f.write("#Doppel\tOCC\n")
    for i in rr:
        f.write("%d"% i)
        f.write("\t")
        do(f, i, contention, ncores, cpu_args, 0)
        do(f, i, contention, ncores, cpu_args, 1)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def products_exp(fnpath, host, rr, ncores):
    fnn = '%s-products-%d-%d-True.data' % (host, rr, ncores)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cont = [1, 10, 100, 1000, 5000, 10000, 100000, 200000, 500000, 1000000]
    if options.short:
        cont = [100, 100000]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\n")
    for i in cont:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, i, ncores, cpu_args, 0)
        do(f, rr, i, ncores, cpu_args, 1)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def print_output(output, prefix, sys):
    x = output.split("Read ")[1]
    y = x.split(":")
    s = prefix + "-" + sys
    s += "\t & " 
    for i, thing in enumerate(y):
        if i%2 == 0:
            continue
        thing = thing[:-4]
        thing = str(int(thing)/1000.0)
        s = s + thing
        s = s + "\\textmu s"
        s = s + " & "
    print s

def run_latency(cmd, prefix, sys):
    if options.dprint:
        print cmd
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Bad status", status, output
        exit(1)
    if options.dprint:
        print output
    print_output(output)


def latency():
    pass

if __name__ == "__main__":
    host = socket.gethostname()
    if len(host.split(".")) > 1:
        host = host.split(".")[0]
    fnpath = 'tmp/'
    if not os.path.exists(fnpath):
        os.mkdir(fnpath)
    if options.exp == "contention":
        contention_exp(fnpath, host, options.default_contention, options.read_rate)
    elif options.exp == "rw":
        rw_exp(fnpath, host, options.default_contention, options.default_ncores)
    elif options.exp == "products":
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
    elif options.exp == "all":
        options.dynamic = True
        if host == "ben":
            options.default_ncores = 40
        elif host == "mat":
            options.default_ncores = 24
        elif host == "tom":
            options.default_ncores = 48
        contention_exp(fnpath, host, options.default_contention, 90)
        contention_exp(fnpath, host, options.default_contention, 10)
        contention_exp(fnpath, host, options.default_contention, 50)
        rw_exp(fnpath, host, options.default_contention, options.default_ncores)
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
