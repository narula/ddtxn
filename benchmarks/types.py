import subprocess

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

class Line(object):
    def __init__(self, points, x, y, title):
        self.points = points
        self.x = x
        self.y = y
        self.title = title


class Gnuplot(object):
    def __init__(self, title, x, y, lines):
        self.lines = lines
        self.title = title
        self.xlabel = x
        self.ylabel = y


    def command_list(self):
        plots = []
        data = []
        for i, curve in enumerate(self.lines):
            title = curve.title
            plots.append("'-' title %s with lp ls %d pt %d" %
                         (title, i+1, i+1))
            plots.append("'-' title '' with errorbars ls %d pt %d" % (i+1, i+1))
            for point in curve.rows():
                data.append("%s %s" % (point.x, point.y))
            for point in curve.rows():
                data.append("%s %s %s %s" % (point.x, point.y, point.min, point.max))
            data.append("e")
        return ["set xlabel %s" % self.xlabel,
                "set ylabel %s" % self.ylabel,
                "plot %s" % ",".join(plots)] + data

    def pdf(self, file=sys.stdout):
        p = subprocess.Popen("gnuplot", stdout=file, stdin=subprocess.PIPE)
        print("set terminal pngcairo\n"
              "set style line 11 lc rgb '#808080' lt 1\n"
              "set border 3 back ls 11\n"
              "set tics nomirror\n"
              "set style line 12 lc rgb '#808080' lt 0 lw 1\n"
              "set grid back ls 12",
              file=p.stdin)
        print("\n".join(self.command_list()), file=p.stdin)
        p.stdin.close()
        p.wait()

            
        
