#!/usr/bin/env python

from __future__ import division
from collections import Counter
from data_graph import *
import psycopg2
import matplotlib.pyplot as plt
import numpy as np	

import elo
import cau
import time

def plot_elo(cur, conn, user_id, color):
	# Fetch elo data for the given user.
	history = elo.elo_history(cur, conn, user_id)
	x = []
	y = []

	# Calculate moving average of elo data if possible.
	window_size = 8
	if len(history) > window_size:
		for i in range(0, len(history) - window_size):
			y_avg = 0
			x_avg = 0
			for j in range(0, window_size):
				entry = history[i + j]
				y_avg += entry[0] / float(window_size)
				x_avg += time.mktime(entry[1].timetuple()) / float(window_size)
			x.append(x_avg)
			y.append(y_avg)
	else:
		for entry in history:
			x.append(time.mktime(entry[1].timetuple()))
			y.append(entry[0])

	# Plot.
	plt.scatter(x, y, color=color)

def plot_cau(cur, conn, user_id, color):
	# Fetch cau data for the given user.
	history = cau.cau_history(cur, conn, user_id)
	x = []
	y = []

	# Calculate moving average of cau data if possible.
	window_size = 8
	if len(history) > window_size:
		for i in range(0, len(history) - window_size):
			y_avg = 0
			x_avg = 0
			for j in range(0, window_size):
				entry = history[i + j]
				y_avg += entry[0] / float(window_size)
				x_avg += time.mktime(entry[1].timetuple()) / float(window_size)
			x.append(x_avg)
			y.append(y_avg)
	else:
		for entry in history:
			x.append(time.mktime(entry[1].timetuple()))
			y.append(entry[0])

	# Plot.
	plt.scatter(x, y, color=color)

def plot_distributions():
	questions = Counter({0: 17663, 1: 3215, 2: 703, 3: 241, 4: 132, 5: 83, 6: 44, 7: 40, 8: 26, 9: 18, 10: 17, 11: 14, 12: 8, 13: 6, 16: 6, 18: 5, 22: 5, 15: 4, 19: 4, 14: 3, 20: 3, 29: 3, 17: 2, 23: 2, 24: 2, 26: 2, 31: 2, 21: 1, 25: 1, 27: 1, 30: 1, 34: 1, 40: 1, 41: 1, 46: 1, 50: 1, 79: 1})
	plt.xscale('log')
	plt.yscale('log')
	plt.scatter(map(lambda x: x+1, questions.keys()), questions.values())
	plt.savefig("output/questions_distribution.png")

	answers = Counter({0: 20315, 1: 1271, 2: 248, 3: 98, 4: 59, 5: 35, 6: 30, 7: 26, 8: 18, 9: 14, 10: 14, 11: 10, 15: 9, 12: 8, 13: 8, 14: 7, 17: 7, 18: 6, 16: 4, 32: 4, 22: 3, 31: 3, 34: 3, 30: 2, 19: 2, 23: 2, 24: 2, 158: 2, 37: 2, 45: 2, 48: 2, 50: 2, 28: 2, 123: 2, 21: 2, 174: 1, 25: 1, 27: 1, 156: 1, 157: 1, 33: 1, 163: 1, 168: 1, 41: 1, 42: 1, 43: 1, 44: 1, 46: 1, 47: 1, 179: 1, 53: 1, 57: 1, 63: 1, 193: 1, 66: 1, 67: 1, 246: 1, 74: 1, 79: 1, 336: 1, 81: 1, 141: 1, 88: 1, 90: 1, 255: 1, 224: 1, 225: 1, 29: 1, 495: 1, 40: 1, 627: 1, 69: 1, 1666: 1, 127: 1})
	plt.xscale('log')
	plt.yscale('log')
	plt.scatter(map(lambda x: x+1, answers.keys()), answers.values())
	plt.savefig("output/answers_distribution.png")

def main(args):
	# Plot cau graph for top 8 users by post count.
	conn, cur = connect("Ben-han", "Ben-han")
	user_ids = [683, 98, 755, 9550, 39, 699, 8321, 4287]
	colors = ['black', 'blue', 'red', 'orange', 'yellow', 'green', 'purple', 'gray']
	for i in range(0, len(user_ids)):
		plot_elo(cur, conn, user_ids[i], colors[i])
	plt.savefig("output/elo.png")
	plt.show()

if __name__ == '__main__':
    main(sys.argv)
