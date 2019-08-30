import numpy as np

dx = .01
x_start = 0
x_end = 1
area = 0

for x in np.arange(x_start, x_end, dx):
	print(x)
	y = x*x+1
	area += dx*y

print(round(area,4))
