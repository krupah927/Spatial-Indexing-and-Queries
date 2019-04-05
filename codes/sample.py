#!/usr/bin/python3

# Sample 10% of lines from an input file and save to an output file

import os
import sys
import random

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {0} InputCSV OutputCSV".format(os.path.basename(sys.argv[0])))
        sys.exit(-1)

    inp = sys.argv[1]  # Input file path
    outp = sys.argv[2]  # Output file path

    # Read all lines from the input file
    with open(inp, "r") as inf:
        all = inf.readlines()
    inf.close()

    total = len(all)  # Total number of lines
    sample = int(float(total) / 10)  # 10% of the input lines

    samples = [all[i] for i in sorted(random.sample(range(total), sample))]

    # Save to output file
    outf = open(outp, "w")
    outf.write("".join(samples))
    outf.close()

    sys.exit(0)
