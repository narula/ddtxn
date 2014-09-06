from __future__ import print_function
import subprocess
import sys
from parse_data import *

# Some of this is stolen from Austin.
#
# Steps:
#
# Read file, wrangle each run into a "point".  Dictionary of key:value
# pairs, where a key is either an arg or a measurement, and value is
# the value for the arg or measurement.
#
# Determine what graph we want to make.  Let's take the single key
# graph.  There are 4 lines, which correspond to pairs of (sys=,
# atomic=).  When creating the first line, sys=0 & atomic=false.  I
# only want points of that type.
#
# However, I also want rr=0 and zipf=-1 and probably a whole lot more
# things.
#
# These are the kwargs passed into reduce_points
#
# For ALL graphs (pretty much): -allocate=false, -rlock=false, -wr=?,
# -phase=80, -sr=10000 -latency=false
#
# For all single_exp graphs: nw=20, rr=0, zipf=-1. x=-contention, y=-total/sec
# Doppel line: sys=0, atomic=false
# OCC line: sys=1, atomic=false
# 2PL : sys=2, atomic=false
# atomic: sys=2, atomic=true
#
# That returns a dictionary where the keys are the xvals of contention
# matching the restrictions and the values are the points.
#
# Turn that into a "Line" object with rows.  Each row is the xval
# (key) and the yval inside the point.
#
# A line also has info about those kwargs (restrictions) and which key
# is x and which key is y.  And a title.

class Point(object):
    def __init__(self, x, y, mn, mx):
        self.x = x
        self.y = y
        self.mn = mn
        self.mx = mx

class Line(object):
    def __init__(self, points, title):
        self.points = points
        self.title = title

    def __getitem__(self, idx):
        if idx >= len(self):
            raise IndexError(idx)
        return self.points[idx]

    def __len__(self):
        return len(self.points)


class Gnuplot(object):
    def __init__(self, title, x, y, lines, key="center right"):
        self.lines = lines
        self.title = title
        self.xlabel = x
        self.ylabel = y
        self.key = key


    def command_list(self):
        plots = []
        data = []
        for i, curve in enumerate(self.lines):
            title = curve.title
            plots.append("'-' title \"%s\" with lp ls %d pt %d" %
                         (title, i+1, i+1))
            plots.append("'-' title '' with errorbars ls %d pt %d" %
                         (i+1, i+1))
            for point in curve:
                data.append("%s %s" % (point.x, point.y))
            data.append("e")
            for point in curve:
                data.append("%s %s %s %s" % (point.x, point.y, point.mn, point.mx))
            data.append("e")
        return ["set xlabel \"%s\"" % self.xlabel,
                "set ylabel \"%s\"" % self.ylabel,
                "set yrange [0:]",
                "plot %s" % ",".join(plots)] + data

    def eps(self, fn):
        p = subprocess.Popen("gnuplot", stdin=subprocess.PIPE)
        print ("set key %s\n" % self.key, file=p.stdin)
        print("set terminal postscript color eps enhanced\n"
              "set output 'x.ps'\n",
              "set size 0.7,0.6\n",
              "set pointsize 1.5\n",
              "set style line 11 lc rgb '#808080' lt 1\n"
              "set border 3 back ls 11\n"
              "set tics nomirror\n"
              "set format y \"%.0sM\"\n"
              "set style line 12 lc rgb '#808080' lt 0 lw 1\n"
              "set grid back ls 12\n",
              file=p.stdin)
        print("\n".join(self.command_list()), file=p.stdin)
        print("system \"epstopdf x.ps\"\n", file=p.stdin)
        print("system \"ps2pdf14 -dPDFSETTINGS=/prepress x.pdf %s\n" % fn, file=p.stdin)
        print("system \"rm x.ps\"\n", file=p.stdin)
        p.stdin.close()
        p.wait()


LINES = [("0", "False"), ("1", "False"), ("2", "False"), ("2", "True")]

def single_graph(all_points):        
    prob = [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    lines = []
    for sys, atomic in LINES:
        points = []
        for n in prob:
            one = all_matching(all_points, nworkers="20", sys=sys, binary="single", atomic=atomic, rr="0", contention=str(n))
            if len(one) == 0:
                raise Exception("Could not get any matching points")
            avg, mn, mx = stat(one, "total/sec")
            if avg is None:
                print (one, sys, atomic, len(points))
                raise Exception("Could not get stats")
            points.append(Point(n, avg, mn, mx))
        lines.append(Line(points, get_title(sys, atomic)))
        points = []
    G = Gnuplot("", "% of transactions with hot key", "Throughput (txns/sec)", lines)
    G.eps("single.pdf")


def per_core_graph(all_points):
    cores = [1, 2, 4, 10, 20, 30, 40, 50, 60, 70, 80]
    lines = []
    for sys, atomic in LINES:
        points = []
        for n in cores:
            one = all_matching(all_points, nworkers=str(n), sys=sys, binary="single", atomic=atomic, rr="0", contention="100")
            if len(one) == 0:
                raise Exception("Could not get any matching points")
            avg, mn, mx = stat(one, "total/sec", fltn("total/sec", n))
            if avg is None:
                print (one, sys, atomic, len(points))
                raise Exception("Could not get stats")
            points.append(Point(n, avg, mn, mx))
        lines.append(Line(points, get_title(sys, atomic)))
        points = []
    G = Gnuplot("", "number of cores", "Throughput (txns/sec)", lines, "top right")
    G.eps("percore.pdf")

def zipf_graph(all_points):
    lines = []
    alphas = ["1e-05", "0.2", "0.4", "0.6", "0.8", "1.00001", "1.2", "1.4", "1.6", "1.8", "2"]
    for sys, atomic in LINES:
        points = []
        for a in alphas:
            one = all_matching(all_points, nworkers="20", sys=sys, binary="single", atomic=atomic, rr="0", contention="-1", zipf=a)
            if len(one) == 0:
                raise Exception("Could not get any matching points", a)
            avg, mn, mx = stat(one, "total/sec", flt("total/sec"))
            if avg is None:
                print (one, sys, atomic, len(points))
                raise Exception("Could not get stats")
            points.append(Point(a, avg, mn, mx))
        lines.append(Line(points, get_title(sys, atomic)))
        points = []
    G = Gnuplot("", "{/Symbol a}", "Throughput (txns/sec)", lines, "bottom left")
    G.eps("zipf.pdf")


def rw_graph(all_points):
    lines = []
    rw = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    for sys, atomic in LINES:
        if atomic == "True":
            continue
        points = []
        for r in rw:
            one = all_matching(all_points, nworkers="20", sys=sys, binary="buy", atomic="False", rr=str(r), contention="-1", zipf="1.4")
            if len(one) == 0:
                raise Exception("Could not get any matching points", r)
            avg, mn, mx = stat(one, "total/sec", flt("total/sec"))
            if avg is None:
                print (one, sys, atomic, len(points))
                raise Exception("Could not get stats")
            points.append(Point(r, avg, mn, mx))
        lines.append(Line(points, get_title(sys, atomic)))
        points = []
    G = Gnuplot("", "\% of transactions that write", "Throughput (txns/sec)", lines, "center right")
    G.eps("rw.pdf")

if __name__ == "__main__":
    f = open('single-data.out.1', 'r')
    all_points = wrangle_file(f)
    single_graph(all_points)
    f = open('single-data.out', 'r')
    all_points = wrangle_file(f)
    per_core_graph(all_points)

    f = open('single-data.out', 'r')
    all_points = wrangle_file(f)
    zipf_graph(all_points)

    f = open('buy-data.out', 'r')
    all_points = wrangle_file(f)
    rw_graph(all_points)
