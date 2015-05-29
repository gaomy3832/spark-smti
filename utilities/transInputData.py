#! /usr/bin/python

import sys, os

if len(sys.argv) < 3:
    print('Translate input data in AAAI\'15 format to separate config, men, women files.')
    print('Usage: {} <input file> <output directory>'.format(sys.argv[0]))
    sys.exit(1)

infile = sys.argv[1]
outdir = sys.argv[2]

if os.path.exists(outdir):
    raise OSError('Output directory {} already exists!'.format(outdir))
os.makedirs(outdir)

with open(infile, 'r') as fh:
    ctx = [ line.strip() for line in fh ]
    # Filter empty lines
    ctx = [ line for line in ctx if line ]

# First line
config = ctx[0]
size = int(config.split()[0])
assert size * 2 + 1 == len(ctx)

# Men preference matrix, index starts from 1
menlist = ctx[1:1+size]
menlist = [ '{}: {}\n'.format(idx+1, menlist[idx]) for idx in xrange(size) ]

# Women preference matrix, index starts from 1
womenlist = ctx[1+size:1+2*size]
womenlist = [ '{}: {}\n'.format(idx+1, womenlist[idx]) for idx in xrange(size) ]

with open(os.path.join(outdir, 'config.txt'), 'w') as fh:
    fh.writelines([config])

with open(os.path.join(outdir, 'men.list'), 'w') as fh:
    fh.writelines(menlist)

with open(os.path.join(outdir, 'women.list'), 'w') as fh:
    fh.writelines(womenlist)

