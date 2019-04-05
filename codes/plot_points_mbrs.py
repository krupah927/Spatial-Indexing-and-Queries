#!/usr/bin/python3

# Scatter plot for trajectories with bounding rectangles

import os
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as patches

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {0} InputCSV OutputPNG".format(os.path.basename(sys.argv[0])))
        sys.exit(-1)

    inp = sys.argv[1]  # Input file path
    outp = sys.argv[2]  # Output file path

    fig = plt.figure(figsize=(12, 9))  # Set figure size to be 12x9 (960x720 pixels)
    ax = fig.add_subplot(111)

    # Read all from input
    x_ps = []  # Xs of points
    y_ps = []  # Ys of points
    with open(inp, "r") as inf:
        for line in inf:
            t = line.split(",")
            if len(t) == 2:
                # Point
                x = float(t[0])  # X
                y = float(t[1])  # Y
                x_ps.append(x)
                y_ps.append(y)
            elif len(t) == 4:
                # Rectangles
                x = float(t[0])  # X
                y = float(t[1])  # Y
                w = float(t[2])  # Width
                h = float(t[3])  # Height
                # Draw a rectangle
                ax.add_patch(
                    patches.Rectangle(
                        (x, y), w, h,
                        fill=False,
                        edgecolor="red"
                    )
                )
            else:
                continue
    inf.close()

    maxX = max(x_ps)
    minX = min(x_ps)
    maxY = max(y_ps)
    minY = min(y_ps)

    x_range = maxX - minY
    y_range = maxY - minY
    delta_x = x_range * 0.05  # Leave some space in each edge
    delta_y = y_range * 0.05  # Leave some space in each edge

    # Plot all points
    ax.scatter(x_ps, y_ps, s=2, c="b", alpha=0.2, marker="*")

    # Range of X and Y axes
    ax.set_xlim(minX - delta_x, maxX + delta_x)
    ax.set_ylim(minY - delta_y, maxY + delta_y)

    # Hide axis labels
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)

    # Save to image
    fig.savefig(outp, transparent=True, bbox_inches="tight")

    # Display
    # plt.show()

    sys.exit(0)
