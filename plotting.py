#!/usr/bin/env python

from __future__ import division
from collections import Counter
from data_graph import *

import matplotlib.pyplot as plt
import search_utilities
import metrics;
import time;
import cau;

def plot_individual_elo(cur, conn, user_id, color):
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

def plot_individual_cau(cur, conn, user_id, color):
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

def plot_avg_auth(cur, user_ids, color, label = "Auth"):
	avg_auth = [0] * len(metrics.percentiles)
	for user_id in user_ids:
		user_auth = metrics.auth_for_user(cur, user_id)
		avg_auth = [sum(x) for x in zip(avg_auth, user_auth)]
	avg_auth = [total / len(user_ids) for total in avg_auth]
	plt.scatter(metrics.percentiles, avg_auth, color = color, label = label)

def plot_avg_pagerank(cur, user_ids, color, label = "PageRank"):
	avg_rank = [0] * len(metrics.percentiles)
	for user_id in user_ids:
		user_rank = metrics.pagerank_for_user(cur, user_id)
		avg_rank = [sum(x) for x in zip(avg_rank, user_rank)]
	avg_rank = [total / len(user_ids) for total in avg_rank]
	plt.scatter(metrics.percentiles, avg_rank, color = color, label = label)

def plot_avg_elo(cur, conn, user_ids, color, label = "ELO"):
	avg_elo = [0] * len(metrics.percentiles)
	for user_id in user_ids:
 		user_elo = metrics.elo_for_user(cur, conn, user_id)
 		avg_elo = [sum(x) for x in zip(avg_elo, user_elo)]
 	avg_elo = [total / len(user_ids) for total in avg_elo]
	plt.scatter(metrics.percentiles, avg_elo, color = color, label = label)

def plot_avg_cau(cur, conn, user_ids, color, label = "CAU"):
	avg_cau = [0] * len(metrics.percentiles)
	for user_id in user_ids:
 		user_cau = metrics.cau_for_user(cur, conn, user_id)
 		avg_cau = [sum(x) for x in zip(avg_cau, user_cau)]
 	avg_cau = [total / len(user_ids) for total in avg_cau]
	plt.scatter(metrics.percentiles, avg_cau, color = color, label = label)	

def plot_reputation_distribution(cur, color):
	reputation_distrib = search_utilities.count_users_by_reputation(cur)
	x = []
	y = []
	for (reputation, count) in reputation_distrib:
		x.append(reputation)
		y.append(count)
	plt.xscale('log')
	plt.yscale('log')
	plt.scatter(x, y, color = color, label = "Reputation")	

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
	conn, cur = connect("cooking", "Ben-han")
	experts = search_utilities.get_experts()
	nonexperts = search_utilities.get_nonexperts()

	# plot_individual_elo(cur, conn, 95, 'red')
	# plt.savefig("output/codegolf.png")
	# plt.show()

	# Expert plots
	# print "Plotting average expert Auth"
	# plot_avg_auth(cur, experts, 'blue')
	print "Plotting average expert PageRank"
	plot_avg_pagerank(cur, experts, 'green', label = "Expert")
	# print "Plotting average expert ELO"
	# plot_avg_elo(cur, conn, experts, 'blue')
	# print "Plotting average expert Cau"
	# plot_avg_cau(cur, conn, experts, 'purple')
	# plt.legend(loc = 4)
	# plt.savefig("output/expert_combined.png")
	# plt.show()

	# Non-expert plots
	# print "Plotting average nonexpert Auth"
	# plot_avg_auth(cur, nonexperts, 'blue')
	print "Plotting average nonexpert PageRank"
	plot_avg_pagerank(cur, nonexperts, 'blue', label = "Non-expert")
	# print "Plotting average nonexpert ELO"
	# plot_avg_elo(cur, conn, nonexperts, 'red')
	# print "Plotting average nonexpert Cau"
	# plot_avg_cau(cur, conn, nonexperts, 'green')
	# plt.legend(loc = 4)

	#plot_reputation_distribution(cur, 'green')
	
	plt.suptitle("Avg. expert PageRank vs non-expert PageRank")
	plt.xlabel("Lifetime percentage")
	plt.ylabel("Rating")
	plt.legend(loc = 4)

	plt.savefig("output/expert_vs_nonexpert_pagerank.png")
	plt.show()

if __name__ == '__main__':
    main(sys.argv)
