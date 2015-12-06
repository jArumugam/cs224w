#!/usr/bin/env python

from __future__ import division
from collections import Counter
import psycopg2
import matplotlib.pyplot as plt
import numpy as np	


def main():
	questions = Counter({0: 17663, 1: 3215, 2: 703, 3: 241, 4: 132, 5: 83, 6: 44, 7: 40, 8: 26, 9: 18, 10: 17, 11: 14, 12: 8, 13: 6, 16: 6, 18: 5, 22: 5, 15: 4, 19: 4, 14: 3, 20: 3, 29: 3, 17: 2, 23: 2, 24: 2, 26: 2, 31: 2, 21: 1, 25: 1, 27: 1, 30: 1, 34: 1, 40: 1, 41: 1, 46: 1, 50: 1, 79: 1})
	plt.xscale('log')
	plt.yscale('log')
	plt.scatter(map(lambda x: x+1, questions.keys()), questions.values())
	plt.savefig("questions_distribution.png")

	answers = Counter({0: 20315, 1: 1271, 2: 248, 3: 98, 4: 59, 5: 35, 6: 30, 7: 26, 8: 18, 9: 14, 10: 14, 11: 10, 15: 9, 12: 8, 13: 8, 14: 7, 17: 7, 18: 6, 16: 4, 32: 4, 22: 3, 31: 3, 34: 3, 30: 2, 19: 2, 23: 2, 24: 2, 158: 2, 37: 2, 45: 2, 48: 2, 50: 2, 28: 2, 123: 2, 21: 2, 174: 1, 25: 1, 27: 1, 156: 1, 157: 1, 33: 1, 163: 1, 168: 1, 41: 1, 42: 1, 43: 1, 44: 1, 46: 1, 47: 1, 179: 1, 53: 1, 57: 1, 63: 1, 193: 1, 66: 1, 67: 1, 246: 1, 74: 1, 79: 1, 336: 1, 81: 1, 141: 1, 88: 1, 90: 1, 255: 1, 224: 1, 225: 1, 29: 1, 495: 1, 40: 1, 627: 1, 69: 1, 1666: 1, 127: 1})
	plt.xscale('log')
	plt.yscale('log')
	plt.scatter(map(lambda x: x+1, answers.keys()), answers.values())
	plt.savefig("answers_distribution.png")
