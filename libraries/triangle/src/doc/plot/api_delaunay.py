import matplotlib.pyplot as plt
import numpy as np
import triangle as tr

pts = np.array([[0, 0], [0, 1], [0.5, 0.5], [1, 1], [1, 0]])
tri = tr.delaunay(pts)

A = {"vertices": pts}
B = {"vertices": pts, "triangles": tri}
tr.compare(plt, A, B)
plt.show()
