# To run the paper experiments, where N=# of cores for the not-scalability experiments:
#  python bm.py --exp=all --rlock --ncores=N

from optparse import OptionParser
import commands
import os
from os import system
import socket

parser = OptionParser()
parser.add_option("-p", "--print", action="store_true", dest="dprint", default=True)
parser.add_option("--exp", action="store", type="string", dest="exp", default="single")
parser.add_option("--short", action="store_true", dest="short", default=False)
parser.add_option("--allocate", action="store_true", dest="allocate", default=False)
parser.add_option("--ncores", action="store", type="int", dest="default_ncores", default=-1)
parser.add_option("--nsec", action="store", type="int", dest="nsec", default=10)
parser.add_option("--contention", action="store", type="float", dest="default_contention", default=1)
parser.add_option("--rr", action="store", type="int", dest="read_rate", default=0)
parser.add_option("--latency", action="store_true", dest="latency", default=False)
parser.add_option("--rlock", action="store_false", dest="rlock", default=True)
parser.add_option("--wratio", action="store", type="float", dest="wratio", default=2.0)
parser.add_option("--sr", action="store", type="int", dest="sr", default=500)
parser.add_option("--phase", action="store", type="int", dest="phase", default=20)
parser.add_option("--zipf", action="store", type="float", dest="zipf", default=-1)
parser.add_option("--partition", action="store_true", dest="partition", default=False)
parser.add_option("--ncrr", action="store", type="float", dest="not_contended_read_rate", default=0.0)
parser.add_option("--cw", action="store", type="float", dest="conflict_weight", default=1.0)
parser.add_option("--version", action="store", type="int", dest="version", default=0)

(options, args) = parser.parse_args()

CPU_ARGS = ""
ben_list_cpus = "thread==0 socket@0,1,2,7,3-6"

LATENCY_PART = " -latency=%s" % options.latency
VERSION_PART = " -v=%d" % options.version

BASE_CMD = "GOGC=off numactl -C `list-cpus seq -n %d %s` ./%s -nprocs=%d -ngo=%d -nw=%d -nsec=%d -contention=%s -rr=%d -allocate=%s -sys=%d -rlock=%s -wr=%s -phase=%s -sr=%d -atomic=%s -zipf=%s -out=data.out -ncrr=%s -cw=%.2f -split=%s" + LATENCY_PART + VERSION_PART

def do_param(bn, rr, contention, ncpu, sys, wratio=options.wratio, phase=options.phase, atomic=False, zipf=-1, ncrr=options.not_contended_read_rate, yval="total/sec", cw=options.conflict_weight, split=False):
    cmd = fill_cmd(bn, rr, contention, ncpu, sys, wratio, phase, atomic, zipf, ncrr, cw, split)

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
        if yval in f:
            x = f.split(":")[1]
    lat = float(x)

def run_one(cmd):
    if options.dprint:
        print cmd
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Bad status", status, output
        exit(1)
    if options.dprint:
        print output

def get_cpus(host):
    ncpus = [1, 2, 4, 8]
    if host == "mat":
        ncpus = [1, 2, 4, 8, 12, 24]
    elif host == "tbilisi":
        ncpus = [1, 2, 4, 8, 12]
    elif host == "tom":
        ncpus = [1, 2, 6, 12, 18]
    elif host == "ben":
        ncpus = [1, 2, 4, 10, 20, 30, 40, 50, 60, 70, 80]
    if options.short:
        ncpus=[2, 4]
    return ncpus

def fill_cmd(bn, rr, contention, ncpus, systype, wratio, phase, atomic, zipf, ncrr, cw, split):
    nsec = options.nsec
    if options.short:
        nsec = 1
    cmd = BASE_CMD % (ncpus, CPU_ARGS, bn, ncpus, ncpus, ncpus, nsec, contention, rr, options.allocate, systype, options.rlock, wratio, phase, options.sr, atomic, zipf, ncrr, cw, split)
    if options.exp.find("single") == 0:
        # Zipf experiments are already not partitioned.
        cmd = cmd + " -partition=%s" % options.partition
    return cmd

def do(bn, rr, contention, ncpu, sys, wratio=options.wratio, phase=options.phase, atomic=False, zipf=-1, ncrr=options.not_contended_read_rate, cw=options.conflict_weight, split=False):
    cmd = fill_cmd(bn, rr, contention, ncpu, sys, wratio, phase, atomic, zipf, ncrr, cw, split)
    run_one(cmd)

def num_keys_exp():
    keys = [1, 10, 20, 50, 70, 100, 110]
    for k in keys:
     cmd = fill_cmd("single", 0, 0, 20, 0, options.wratio, options.phase, False, -1, 0, options.conflict_weight, False)
     cmd = cmd + " -nb=%d" % k
     run_one(cmd)
     cmd = fill_cmd("single", 0, 0, 20, 1, options.wratio, options.phase, False, -1, 0, options.conflict_weight, False)
     cmd = cmd + " -nb=%d" % k
     run_one(cmd)

def wratio_exp(host, contention, rr):
    cpus = get_cpus(host)
    for i in cpus:
        do("buy", rr, contention, i, 0, wratio=2)
        do("buy", rr, contention, i, 0, wration=3)
        do("buy", rr, contention, i, 0, wratio=4)
        do("buy", rr, contention, i, 0, wratio=5)
        do("buy", rr, contention, i, 1)

def bad_exp(rr, ncores=options.default_ncores):
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
    for i in theta:
        do("buy", rr, -1, ncores, 0, zipf=i)
        do("buy", rr, -1, ncores, 1, zipf=i)
        do("buy", rr, -1, ncores, 2, zipf=i)

def ncrr_exp(rr, ncores):
    ncrr = [0, .2, .4, .6, .8, 1.0]
    for i in ncrr:
        do("buy", rr, -1, ncores, 0, zipf=1.4, ncrr=i)
        do("buy", rr, -1, ncores, 1, zipf=1.4, ncrr=i)
        do("buy", rr, -1, ncores, 2, zipf=1.4, ncrr=i)

def single_reads_exp(ncores):
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50, 100]
    for i in rr:
        do("single", 50, i, ncores, 0)
        do("single", 50, i, ncores, 1)
        do("single", 50, i, ncores, 2)

def single_rw_exp(ncores):
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50]
    for i in rr:
        do("single", i, 100, ncores, 0)
        do("single", i, 100, ncores, 1)
        do("single", i, 100, ncores, 2)
        do("single", i, 100, ncores, 0, wratio=.5, cw=100.0, split=True)

def contention_exp(host, contention, rr, zipf=-1):
    cpus = get_cpus(host)
    for i in cpus:
        do("buy", rr, contention, i, 0, zipf=zipf)
        do("buy", rr, contention, i, 1, zipf=zipf)
        do("buy", rr, contention, i, 2, zipf=zipf)

def zipf_scale_exp(host, zipf, rr):
    cpus = get_cpus(host)
    for i in cpus:
        do("single", rr, -1, i, 0, zipf=zipf)
        do("single", rr, -1, i, 1, zipf=zipf)
        do("single", rr, -1, i, 2, zipf=zipf)

def rw_exp(contention, ncores, zipf, ncrr):
    if zipf > 0:
        contention = -1
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50, 100]
    for i in rr:
        do("buy", i, contention, ncores, 0, zipf=zipf, ncrr=ncrr)
        do("buy", i, contention, ncores, 1, zipf=zipf, ncrr=ncrr)
        do("buy", i, contention, ncores, 2, zipf=zipf, ncrr=ncrr)

def products_exp(rr, ncores):
    cont = [1, 10, 100, 1000, 10000, 50000, 100000, 200000, 1000000]
    if options.short:
        cont = [100, 100000]
    for i in cont:
        do("buy", rr, i, ncores, 0, zipf=-1)
        do("buy", rr, i, ncores, 1, zipf=-1)
        do("buy", rr, i, ncores, 2, zipf=-1)

def single_exp(rr, ncores):
    prob = [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        prob = [1, 5]
    for i in prob:
        do("single", rr, i, ncores, 0, zipf=-1)
        do("single", rr, i, ncores, 1, zipf=-1)
        do("single", rr, i, ncores, 2, zipf=-1)
        do("single", rr, i, ncores, 2, zipf=-1, atomic=True)

def buy_exp(host):
    cpus = get_cpus(host)
    for i in cpus:
        if i < 30:
            continue
        do("buy", 0, -1, i, 0, zipf=1.4)
        do("buy", 0, -1, i, 1, zipf=1.4)
        do("buy", 0, -1, i, 2, zipf=1.4)

def single_scale_exp(host, contention, rr, zipf):
    cpus = get_cpus(host)
    if zipf != -1:
        contention = -1
    for i in cpus:
        do("single", rr, contention, i, 0, zipf=zipf)
        do("single", rr, contention, i, 1, zipf=zipf)
        do("single", rr, contention, i, 2, zipf=zipf)
        do("single", rr, contention, i, 2, zipf=-1, atomic=True)

def zipf_exp(rr, ncores):
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
    for i in theta:
        do("single", rr, -1, ncores, 0, zipf=i)
        do("single", rr, -1, ncores, 1, zipf=i)
        do("single", rr, -1, ncores, 2, zipf=i)
        do("single", rr, -1, ncores, 2, atomic=True, zipf=i)

def phase_exp(ncores):
    phase_len = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        phase_len = [20, 160]
    for i in phase_len:
        do_param("buy", 10, -1, ncores, 0, phase=i, zipf=1.4, yval="Read Avg", ncrr=0)
        do_param("buy", 50, -1, ncores, 0, phase=i, zipf=1.4, yval="Read Avg", ncrr=0)
        do_param("buy", 50, 1, ncores, 0, phase=i, zipf=-1, yval="Read Avg", ncrr=0)

def phase_tps_exp(ncores):
    phase_len = [1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        phase_len = [20, 160]
    for i in phase_len:
        do("buy", 10, -1, ncores, 0, phase=i, zipf=1.4, ncrr=0)
        do("buy", 50, -1, ncores, 0, phase=i, zipf=1.4, ncrr=0)
        do("buy", 50, 1, ncores, 0, phase=i, zipf=-1, ncrr=0)

def rubis_exp(host, contention, ncores):
    contention = int(contention)
    ncrr = [3.7, 20, 40, 60, 80]
    for i in ncrr:
        do("rubis", 0, contention, ncores, 0, zipf=1.4, ncrr=i)
        do("rubis", 0, contention, ncores, 1, zipf=1.4, ncrr=i)
        do("rubis", 0, contention, ncores, 2, zipf=-1, ncrr=i)

def rubisz_exp(ncores, ncrr):
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
    for i in theta:
        do("rubis", 0, 30, ncores, 0, zipf=i, ncrr=ncrr)
        do("rubis", 0, 30, ncores, 1, zipf=i, ncrr=ncrr)
        do("rubis", 0, 30, ncores, 2, zipf=i, ncrr=ncrr)

if __name__ == "__main__":
    host = socket.gethostname()
    if len(host.split(".")) > 1:
        host = host.split(".")[0]
    if options.default_ncores == -1:
        if host == "ben":
            options.default_ncores = 20
        elif host == "mat":
            options.default_ncores = 24
        elif host == "tom":
            options.default_ncores = 18
        elif host == "tbilisi":
            options.default_ncores = 12

    if host == "ben":
        CPU_ARGS = ben_list_cpus

    # figure 8
    if options.exp == "single" or options.exp == "all":
        single_exp(0, options.default_ncores)        
    # figure 9
    if options.exp == "singlescale" or options.exp == "all":
        single_scale_exp(host, 100, 0, -1)
    # figure 10
    # figure 11
    if options.exp == "zipf" or options.exp == "all":
        zipf_exp(0, options.default_ncores)
    # figure 12
    if options.exp == "rw" or options.exp == "all":
        rw_exp(-1, options.default_ncores, 1.4, 0.0)
    # figure 13
    if options.exp == "phase" or options.exp == "all":
        phase_exp(options.default_ncores)
    # figure 14
    if options.exp == "phasetps" or options.exp == "all":
        phase_tps_exp(options.default_ncores)
    # figure 15
    if options.exp == "rubisz" or options.exp == "all":
        if options.not_contended_read_rate == 0:
            options.not_contended_read_rate = 3.7
        rubisz_exp(options.default_ncores, options.not_contended_read_rate)

    if options.exp == "all":
        exit()

    # not in the paper or talk
    if options.exp == "bad":
        bad_exp(options.read_rate)
    if options.exp == "singlereads":
        single_reads_exp(options.default_ncores)
    if options.exp == "singlerw":
        single_rw_exp(options.default_ncores)
    if options.exp == "buy":
        buy_exp(host)
    if options.exp == "contention":
        if options.read_rate == -1:
            contention_exp(host, options.default_contention, 90, options.zipf)
            contention_exp(host, options.default_contention, 10, options.zipf)
            contention_exp(host, options.default_contention, 50, options.zipf)
        else:
            contention_exp(host, options.default_contention, options.read_rate, options.zipf)
    if options.exp == "products":
        products_exp(options.read_rate, options.default_ncores)
    if options.exp == "numkeys":
        num_keys_exp()
    if options.exp == "wratio":
        wratio_exp(host, options.default_contention, options.read_rate)
    if options.exp == "ncrr":
        ncrr_exp(options.read_rate, options.default_ncores)
    if options.exp == "likescale":
        zipf_scale_exp(host, 0.6, 50)
        zipf_scale_exp(host, 1.001, 50)
        zipf_scale_exp(host, 1.4, 50)
    if options.exp == "rubis":
        rubis_exp(host, 3, options.default_ncores)
    if options.exp == "zipfscale":
        zipf_scale_exp(host, options.zipf, options.read_rate)
