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
parser.add_option("--nsec", action="store", type="int", dest="nsec", default=10)
parser.add_option("--contention", action="store", type="float", dest="default_contention", default=100000)
parser.add_option("--rr", action="store", type="int", dest="read_rate", default=0)
parser.add_option("--latency", action="store_true", dest="latency", default=False)
parser.add_option("--rlock", action="store_false", dest="rlock", default=True)
parser.add_option("--scp", action="store_true", dest="scp", default=True)
parser.add_option("--noscp", action="store_false", dest="scp")
parser.add_option("--wratio", action="store", type="float", dest="wratio", default=2.0)
parser.add_option("--sr", action="store", type="int", dest="sr", default=500)
parser.add_option("--phase", action="store", type="int", dest="phase", default=20)
parser.add_option("--zipf", action="store", type="float", dest="zipf", default=-1)
parser.add_option("--partition", action="store_true", dest="partition", default=False)
parser.add_option("--ncrr", action="store", type="float", dest="not_contended_read_rate", default=0.0)
parser.add_option("--cw", action="store", type="float", dest="conflict_weight", default=1.0)
parser.add_option("--version", action="store", type="int", dest="version", default=0)

(options, args) = parser.parse_args()

ben_list_cpus = "socket@0,1,2,7,3-6"

LATENCY_PART = " -latency=%s" % options.latency
VERSION_PART = " -v=%d" % options.version

BASE_CMD = "GOGC=off numactl -C `list-cpus seq -n %d %s` ./%s -nprocs=%d -ngo=%d -nw=%d -nsec=%d -contention=%s -rr=%d -allocate=%s -sys=%d -rlock=%s -wr=%s -phase=%s -sr=%d -atomic=%s -zipf=%s -out=data.out -ncrr=%s -cw=%.2f -split=%s" + LATENCY_PART + VERSION_PART

def do_param(fn, rr, contention, ncpu, list_cpus, sys, wratio=options.wratio, phase=options.phase, atomic=False, zipf=-1, ncrr=options.not_contended_read_rate, yval="total/sec", cw=options.conflict_weight, split=False):
    cmd = fill_cmd(rr, contention, ncpu, sys, list_cpus, wratio, phase, atomic, zipf, ncrr, cw, split)

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
    fn.write("%0.2f\t" % lat)

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
        ncpus = [1, 2, 4, 10, 20, 30, 40, 50, 60, 70, 80]
    if options.short:
        ncpus=[2, 4]
    return ncpus

def fill_cmd(rr, contention, ncpus, systype, cpus_arg, wratio, phase, atomic, zipf, ncrr, cw, split):
    nsec = options.nsec
    if options.short:
        nsec = 1
    bn = "buy"
    if options.exp.find("rubis") == 0:
        bn = "rubis"
    if options.exp == "zipf" or options.exp.find("single") == 0:
        bn = "single"
    xncpus = ncpus
    cmd = BASE_CMD % (xncpus, cpus_arg, bn, xncpus, ncpus, ncpus, nsec, contention, rr, options.allocate, systype, options.rlock, wratio, phase, options.sr, atomic, zipf, ncrr, cw, split)
    if options.exp.find("single") == 0:
        # Zipf experiments are already not partitioned.
        cmd = cmd + " -partition=%s" % options.partition
    return cmd

def do(f, rr, contention, ncpu, list_cpus, sys, wratio=options.wratio, phase=options.phase, atomic=False, zipf=-1, ncrr=options.not_contended_read_rate, cw=options.conflict_weight, split=False):
    cmd = fill_cmd(rr, contention, ncpu, sys, list_cpus, wratio, phase, atomic, zipf, ncrr, cw, split)
    run_one(f, cmd)
    f.write("\t")

def wratio_exp(fnpath, host, contention, rr):
    fnn = '%s-wratio-%d-%d-%s.data' % (host, contention, rr, True)
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

def bad_exp(fnpath, host, rr, ncores=options.default_ncores):
    fnn = '%s-bad-%d.data' % (host, rr)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
    for i in theta:
        f.write("%0.2f"% i)
        f.write("\t")
        do(f, rr, -1, ncores, cpu_args, 0, zipf=i)
        do(f, rr, -1, ncores, cpu_args, 1, zipf=i)
        do(f, rr, -1, ncores, cpu_args, 2, zipf=i)
        f.write("\n")
    f.close()

def ncrr_exp(fnpath, host, rr, ncores):
    fnn = '%s-ncrr-%d.data' % (host, rr)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    ncrr = [0, .2, .4, .6, .8, 1.0]
    for i in ncrr:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, -1, ncores, cpu_args, 0, zipf=1.4, ncrr=i)
        do(f, rr, -1, ncores, cpu_args, 1, zipf=1.4, ncrr=i)
        do(f, rr, -1, ncores, cpu_args, 2, zipf=1.4, ncrr=i)
        f.write("\n")
    f.close()

def single_reads_exp(fnpath, host, ncores):
    fnn = '%s-singler.data' % (host)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50, 100]
    for i in rr:
        f.write("%d"% i)
        f.write("\t")
        do(f, 50, i, ncores, cpu_args, 0)
        do(f, 50, i, ncores, cpu_args, 1)
        do(f, 50, i, ncores, cpu_args, 2)
        f.write("\n")
    f.close()

def single_rw_exp(fnpath, host, ncores):
    fnn = '%s-singleh.data' % (host)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50]
    for i in rr:
        f.write("%d"% i)
        f.write("\t")
        do(f, i, 100, ncores, cpu_args, 0)
        do(f, i, 100, ncores, cpu_args, 1)
        do(f, i, 100, ncores, cpu_args, 2)
        do(f, i, 100, ncores, cpu_args, 0, wratio=.5, cw=100.0, split=True)
        f.write("\n")
    f.close()

# x-axis is # cores
def contention_exp(fnpath, host, contention, rr, zipf=-1):
    fnn = '%s-scalability-%d-%d-%.2f.data' % (host, contention, rr, zipf)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, contention, i, cpu_args, 0, zipf=zipf)
        do(f, rr, contention, i, cpu_args, 1, zipf=zipf)
        do(f, rr, contention, i, cpu_args, 2, zipf=zipf)
        f.write("\n")
    f.close()

def zipf_scale_exp(fnpath, host, zipf, rr):
    fnn = '%s-zipf-scale-%.02f-%d-%s.data' % (host, zipf, rr, True)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#Doppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, -1, i, cpu_args, 0, zipf=zipf)
        do(f, rr, -1, i, cpu_args, 1, zipf=zipf)
        do(f, rr, -1, i, cpu_args, 2, zipf=zipf)
        f.write("\n")
    f.close()

def rw_exp(fnpath, host, contention, ncores, zipf, ncrr):
    fnn = '%s-rw-%d-%d-%.2f.data' % (host, contention, ncores, options.zipf)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    rr = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        rr = [0, 50, 100]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    f.write("#Doppel\tOCC\t2PL\n")
    for i in rr:
        f.write("%d"% i)
        f.write("\t")
        do(f, i, contention, ncores, cpu_args, 0, zipf=zipf, ncrr=ncrr)
        do(f, i, contention, ncores, cpu_args, 1, zipf=zipf, ncrr=ncrr)
        do(f, i, contention, ncores, cpu_args, 2, zipf=zipf, ncrr=ncrr)
        f.write("\n")
    f.close()

def products_exp(fnpath, host, rr, ncores):
    fnn = '%s-products-%d-%d-%s.data' % (host, rr, ncores, True)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cont = [1, 10, 100, 1000, 10000, 50000, 100000, 200000, 1000000]
    if options.short:
        cont = [100, 100000]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\t2PL\n")
    for i in cont:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, i, ncores, cpu_args, 0, zipf=-1)
        do(f, rr, i, ncores, cpu_args, 1, zipf=-1)
        do(f, rr, i, ncores, cpu_args, 2, zipf=-1)
        f.write("\n")
    f.close()

def single_exp(fnpath, host, rr, ncores):
    fnn = '%s-single-%d-%s-%s.data' % (host, ncores, True, not options.partition)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    prob = [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        prob = [1, 5]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\t2PL\tAtomic\n")
    for i in prob:
        f.write("%0.2f"% i)
        f.write("\t")
        do(f, rr, i, ncores, cpu_args, 0, zipf=-1)
        do(f, rr, i, ncores, cpu_args, 1, zipf=-1)
        do(f, rr, i, ncores, cpu_args, 2, zipf=-1)
        do(f, rr, i, ncores, cpu_args, 2, zipf=-1, atomic=True)
        f.write("\n")
    f.close()

def single_scale_exp(fnpath, host, contention, rr):
    fnn = '%s-single-scale-%d-%d-X.data' % (host, contention, rr)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\t2PL\tAtomic\n")
    for i in cpus:
        f.write("%d"% i)
        f.write("\t")
        do(f, rr, contention, i, cpu_args, 0, zipf=-1)
        do(f, rr, contention, i, cpu_args, 1, zipf=-1)
        do(f, rr, contention, i, cpu_args, 2, zipf=-1)
        do(f, rr, contention, i, cpu_args, 2, zipf=-1, atomic=True)
        f.write("\n")
    f.close()

def zipf_exp(fnpath, host, rr, ncores):
    fnn = '%s-zipf.data' % (host)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
    cpus = [20,]# 40, 80]
    sys = [0, 1, 2]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\tOCC\t2PL\tDoppel-40\tOCC-40\t2PL-40\tDoppel-80\tOCC-80\t2PL-80\n")
    for i in theta:
        f.write("%0.2f"% i)
        for j in cpus:
            for s in sys:
                f.write("\t")
                do(f, rr, -1, j, cpu_args, s, zipf=i)
            f.write("\t")
            do(f, rr, -1, j, cpu_args, 2, atomic=True, zipf=i)
        f.write("\n")
    f.close()

def phase_exp(fnpath, host, ncores):
    fnn = '%s-phase-%d.data' % (host, ncores)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    phase_len = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        phase_len = [20, 160]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\n")
    for i in phase_len:
        f.write("%d"% i)
        f.write("\t")
        do_param(f, 10, -1, ncores, cpu_args, 0, phase=i, zipf=1.4, yval="Read Avg", ncrr=0)
        do_param(f, 50, -1, ncores, cpu_args, 0, phase=i, zipf=1.4, yval="Read Avg", ncrr=0)
        do_param(f, 50, 1, ncores, cpu_args, 0, phase=i, zipf=-1, yval="Read Avg", ncrr=0)
        f.write("\n")
    f.close()

def phase_tps_exp(fnpath, host, ncores):
    fnn = '%s-phase-tps-%d.data' % (host, ncores)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    phase_len = [1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    if options.short:
        phase_len = [20, 160]
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus

    f.write("#Doppel\n")
    for i in phase_len:
        f.write("%d"% i)
        f.write("\t")
        do(f, 10, -1, ncores, cpu_args, 0, phase=i, zipf=1.4, ncrr=0)
        do(f, 50, -1, ncores, cpu_args, 0, phase=i, zipf=1.4, ncrr=0)
        do(f, 50, 1, ncores, cpu_args, 0, phase=i, zipf=-1, ncrr=0)
        f.write("\n")
    f.close()
    

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

def rubis_exp(fnpath, host, contention, ncores):
    contention = int(contention)
    fnn = '%s-rubis-%d.data' % (host, contention)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    f.write("#\tDoppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
#    ncrr = [3.7, 20, 60, 80]
    ncrr = [3.7, 20, 40, 60, 80]
    for i in ncrr:
        f.write("%d"% i)
        f.write("\t")
        do(f, 0, contention, ncores, cpu_args, 0, zipf=1.4, ncrr=i)
        do(f, 0, contention, ncores, cpu_args, 1, zipf=1.4, ncrr=i)
#        do(f, 0, contention, ncores, cpu_args, 2, zipf=-1, ncrr=i)
        f.write("\n")
    f.close()
    if options.scp:
        system("scp %s tbilisi.csail.mit.edu:/home/neha/src/txn/src/txn/data/" % filename)
        system("scp %s tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/graphs/" % filename)

def rubisz_exp(fnpath, host, ncores, ncrr):
    fnn = '%s-rubis.data' % (host,)
    filename=os.path.join(fnpath, fnn)
    f = open(filename, 'w')
    cpus = get_cpus(host)
    f.write("#\tDoppel\tOCC\t2PL\n")
    cpu_args = ""
    if host == "ben":
        cpu_args = ben_list_cpus
    theta = [.00001, .2, .4, .6, .8, 1.00001, 1.2, 1.4, 1.6, 1.8, 2.0]
#    theta = [1.6, 1.8, 2.0]
    for i in theta:
        f.write("%d"% i)
        f.write("\t")
        do(f, 0, 30, ncores, cpu_args, 0, zipf=i, ncrr=ncrr)
        do(f, 0, 30, ncores, cpu_args, 1, zipf=i, ncrr=ncrr)
        do(f, 0, 30, ncores, cpu_args, 2, zipf=i, ncrr=ncrr)
        f.write("\n")
    f.close()

if __name__ == "__main__":
    host = socket.gethostname()
    if len(host.split(".")) > 1:
        host = host.split(".")[0]
    fnpath = 'tmp/'
    if not os.path.exists(fnpath):
        os.mkdir(fnpath)
        
    if options.default_ncores == -1:
        if host == "ben":
            options.default_ncores = 20
        elif host == "mat":
            options.default_ncores = 24
        elif host == "tom":
            options.default_ncores = 18
        elif host == "tbilisi":
            options.default_ncores = 12

    if options.exp == "zipfscale":
        zipf_scale_exp(fnpath, host, options.zipf, options.read_rate)
    if options.exp == "contention":
        if options.read_rate == -1:
            contention_exp(fnpath, host, options.default_contention, 90, options.zipf)
            contention_exp(fnpath, host, options.default_contention, 10, options.zipf)
            contention_exp(fnpath, host, options.default_contention, 50, options.zipf)
        else:
            contention_exp(fnpath, host, options.default_contention, options.read_rate, options.zipf)
    elif options.exp == "rw":
        zipf = 1.4
        ncrr = 0.0
        rw_exp(fnpath, host, options.default_contention, options.default_ncores, zipf, ncrr)
    elif options.exp == "phase":
        phase_exp(fnpath, host, options.default_ncores)
    elif options.exp == "phasetps":
        phase_tps_exp(fnpath, host, options.default_ncores)
    elif options.exp == "products":
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
    elif options.exp == "single":
        single_exp(fnpath, host, 0, options.default_ncores)
    elif options.exp == "singlereads":
        single_reads_exp(fnpath, host, options.default_ncores)
    elif options.exp == "singlerw":
        single_rw_exp(fnpath, host, options.default_ncores)
    elif options.exp == "singlescale":
        single_scale_exp(fnpath, host, options.default_contention, options.read_rate)
    elif options.exp == "zipf":
        zipf_exp(fnpath, host, 0, options.default_ncores)
    elif options.exp == "rubis":
        rubis_exp(fnpath, host, 3, options.default_ncores)
    elif options.exp == "rubisz":
        rubisz_exp(fnpath, host, options.default_ncores, options.not_contended_read_rate)
    elif options.exp == "all":
        options.exp = "single"
        single_exp(fnpath, host, 0, options.default_ncores)
        options.exp = "all"
        rw_exp(fnpath, host, options.default_contention, options.default_ncores)
        products_exp(fnpath, host, options.read_rate, options.default_ncores)
        contention_exp(fnpath, host, options.default_contention, 50)
        options.exp="zipf"
        zipf_exp(fnpath, host, 0, options.default_ncores)
        options.exp="rubis"
        rubis_exp(fnpath, host, 100000, 0)
    elif options.exp == "wratio":
        wratio_exp(fnpath, host, options.default_contention, options.read_rate)
    elif options.exp == "bad":
        bad_exp(fnpath, host, options.read_rate)
    elif options.exp == "ncrr":
        ncrr_exp(fnpath, host, options.read_rate, options.default_ncores)
    elif options.exp == "likescale":
        zipf_scale_exp(fnpath, host, 0.6, 50)
        zipf_scale_exp(fnpath, host, 1.001, 50)
        zipf_scale_exp(fnpath, host, 1.4, 50)
    if options.scp and not options.short:
        system("scp data.out tbilisi.csail.mit.edu:/home/neha/doc/ddtxn-doc/data/")
