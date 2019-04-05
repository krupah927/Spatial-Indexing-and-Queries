#!/usr/bin/python3

# Sample points between February and June

import os
import sys
from datetime import datetime

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {0} InputCSV OutputCSV".format(os.path.basename(sys.argv[0])))
        sys.exit(-1)

    inp = sys.argv[1]  # Input file path
    outp = sys.argv[2]  # Output file path

    # Read all lines from the input file
    with open(inp, "r", encoding="utf-8") as inf, open(outp, "w") as outf:
        for line in inf:
            line = line.replace("\r", "").replace("\n", "")
            if len(line) > 0:
                mon_str = line[-14:-12]  # Month
                mon = int(mon_str)
                if mon >= 2 and mon <= 6:
                    # Between February and June
                    outf.write(line + "\n")
    inf.close()
    outf.close()

    sys.exit(0)
