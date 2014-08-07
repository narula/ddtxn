from optparse import OptionParser
import commands
import os
from os import system
import socket

parser = OptionParser()
parser.add_option("-p", "--print", action="store_true", dest="dprint", default=True)
parser.add_option("--exp", action="store", type="string", dest="exp", default="contention")
parser.add_option("--short", action="store_true", dest="short", default=False)
parser.add_option("--allocate", action="store_true", dest="allocate", default=False)
parser.add_option("--ncores", action="store", type="int", dest="default_ncores", default=-1)
parser.add_option("--contention", action="store", type="float", dest="default_contention", default=100000)
parser.add_option("--rr", action="store", type="int", dest="read_rate", default=50)
parser.add_option("--latency", action="store_true", dest="latency", default=False)
parser.add_option("--rlock", action="store_false", dest="rlock", default=True)
parser.add_option("--scp", action="store_true", dest="scp", default=True)
parser.add_option("--wratio", action="store", type="float", dest="wratio", default=4)
parser.add_option("--sr", action="store", type="int", dest="sr", default=10000)
parser.add_option("--phase", action="store", type="int", dest="phase", default=80)
parser.add_option("--retry", action="store_true", dest="retry", default=False)
parser.add_option("--atomic", action="store_true", dest="atomic", default=True)


(options, args) = parser.parse_args()

ben_list_cpus = "socket@0,1,2,7,3-6"

LATENCY_PART = " -latency=%s" % options.latency

BASE_CMD = "GOGC=500 numactl -C `list-cpus seq -n %d %s` ./%s -nprocs %d -ngo %d -nw %d -nsec %d -contention %s -rr %d -allocate=%s -sys=%d -rlock=%s -wr=%s -phase=%s -sr=%d -retry=%s -atomic=%s" + LATENCY_PART

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
        ncpus = [1, 2, 6, 12, 18]
    elif host == "ben":
        ncpus = [1, 4, 10, 20, 30, 40, 50, 60, 70, 80]
    if options.short:
        ncpus=[2, 4]
    return ncpus

def fill_cmd(rr, contention, ncpus, systype, cpus_arg="", wratio=options.wratio, phase=options.phase, retry=options.retry, atomic=options.atomic):
    nsec = 10
    if options.short:
        nsec = 1
    bn = "buy"
    if options.exp == "rubis":
        bn = "rubis"
    if options.exp == "single":
        bn = "single"
    xncpus = ncpus
    if xncpus < 80:
        xncpus += 1
    cmd = BASE_CMD % (xncpus, cpus_arg, bn, xncpus, ncpus, ncpus, nsec, contention, rr, options.allocate, systype, options.rlock, wratio, phase, options.sr, retry, atomic)
    return cmd

def do(f, rr, contention, ncpu, list_cpus, sys, wratio=options.wratio, phase=options.phase, atomic=options.atomic):
    cmd = fill_cmd(rr, contention, ncpu, sys, list_cpus, wratio, phase, atomic=atomic)
    run_one(f, cmd)
    f.write("\t")

def wratio_exp(fnpath, host, contention, rr):
    fnn = '%s-wratio-%d-%d-%s.data' % (host, contention, rr, options.retry)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel-2\tDoppel-3\tDoppel-4\tDoppel-5\tOCC\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, contention, i, cpu_args, 0, 2)
        do(f, rr, contention, i, cpu_args, 0, 3)
        do(f, rr, contention, i, cpu_args, 0, 4)
        do(f, rr, contention, i, cpu_args, 0, 5)
        do(f, rr, contention, i, cpu_args, 1)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

# x-axis is # cores
def contention_exp(fnpath, host, contention, rr):
    fnn = '%s-scalability-%d-%d-%s.data' % (host, contention, rr, options.retry)
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
        do(f, rr, contention, i, cpu_args, 1, atomic=False)
        #do(f, rr, contention, i, cpu_args, 2)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)


def rw_exp(fnpath, host, contention, ncores):
    fnn = '%s-rw-%d-%d-%s.data' % (host, contention, ncores, options.retry)
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
        do(f, i, contention, ncores, cpu_args, 1, atomic=False)
        #do(f, i, contention, ncores, cpu_args, 2)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def products_exp(fnpath, host, rr, ncores):
    fnn = '%s-products-%d-%d-%s.data' % (host, rr, ncores, options.retry)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cont = [1, 10, 100, 1000, 10000, 50000, 100000, 200000, 1000000]
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
        do(f, rr, i, ncores, cpu_args, 1, atomic=False)
        #do(f, rr, i, ncores, cpu_args, 2)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def single_exp(fnpath, host, rr, ncores):
    fnn = '%s-single-%d-%s.data' % (host, ncores, options.retry)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    prob = [0, 1, 3, 5, 10, 20, 50]
    if options.short:
        prob = [1, 5]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\n")
    for i in prob:
        f.write("%0.2f"% i)
        f.write("\t")
        do(f, rr, i, ncores, cpu_args, 0)
        do(f, rr, i, ncores, cpu_args, 1)
        do(f, rr, i, ncores, cpu_args, 1, atomic=False)
        #do(f, rr, i, ncores, cpu_args, 2)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def phase_exp(fnpath, host, contention, rr, ncores):
    fnn = '%s-phase-%d-%d-%d-%s.data' % (host, contention, rr, ncores, options.retry)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    phase_len = [5, 10, 20, 40, 80, 120, 160, 200]
    if options.short:
        phase_len = [20, 160]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\n")
    for i in phase_len:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, 10, ncores, cpu_args, 0, options.wratio, i)
        do(f, rr, contention, ncores, cpu_args, 0, options.wratio, i)
        do(f, 10, contention, ncores, cpu_args, 0, options.wratio, i)
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

def rubis_exp(fnpath, host, contention, rr):
    fnn = '%s-rubis-%d-%d-%s.data' % (host, contention, rr, options.retry)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#\tDoppel\tOCC\n")
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


if __name__ == "__main__":
    host = socket.gethostname()
    if len(host.split(".")) > 1:
        host = host.split(".")[0]
    fnpath = 'tmp/'
    if not os.path.exists(fnpath):
        os.mkdir(fnpath)
        
    if options.default_ncores == -1:
        if host == "ben":
            options.default_ncores = 40
        elif host == "mat":
            options.default_ncores = 24
        elif host == "tom":
            options.default_ncores = 18

    if options.exp == "contention":
        if options.read_rate == -1:
            contention_exp(fnpath, host, options.default_contention, 90)
            contention_exp(fnpath, host, options.default_contention, 10)
            contention_exp(fnpath, host, options.default_contention, 50)
        else:
            contention_exp(fnpath, host, options.default_contention, options.read_rate)
    elif options.exp == "rw":
        rw_exp(fnpath, host, options.default_contention, options.default_ncores)
    elif options.exp == "phase":
        phase_exp(fnpath, host, options.default_contention, options.read_rate, options.default_ncores)
    elif options.exp == "products":
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
    elif options.exp == "single":
        single_exp(fnpath, host, 0, options.default_ncores)
    elif options.exp == "rubis":
        if options.read_rate == -1:
            rubis_exp(fnpath, host, 30, 90)
            rubis_exp(fnpath, host, 1000000, 50)
        else:
            rubis_exp(fnpath, host, options.default_contention, options.read_rate)
    elif options.exp == "all":
        options.exp = "single"
        single_exp(fnpath, host, 0, options.default_ncores)
        options.exp = "all"
        rw_exp(fnpath, host, options.default_contention, options.default_ncores)
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
        contention_exp(fnpath, host, options.default_contention, 90)
        contention_exp(fnpath, host, options.default_contention, 10)
        contention_exp(fnpath, host, options.default_contention, 50)
    elif options.exp == "wratio":
        wratio_exp(fnpath, host, options.default_contention, options.read_rate)
