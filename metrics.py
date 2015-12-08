#!/usr/bin/env python

from __future__ import division
from collections import Counter
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import graph2
import elo
import cau

DB_NAME = "stackexchangedb"
DB_USER = "postgres"

percentiles = [i*0.01 for i in range(0, 101)]

def connect(db=DB_NAME, user=DB_USER):
    """Connect to the specified Postgres database as the specified user."""
    conn = psycopg2.connect("dbname={} user={}".format(db, user))
    cur = conn.cursor()
    return conn, cur

def results(cursor):
    """Generator for iterating over query results one at a time."""
    while True:
        result = cursor.fetchone()
        if result is None:
            break
        yield result

def get_start_time(userID, cur):
    query = "select creation_date from se_user where id = %(id)s"
    cur.execute(query, {'id': userID})
    return [i[0] for i in results(cur)][0]

def percentile_normalization(userID, cur, sampling_percentiles):
    """returns a vector of end times for percentiles 0, 10, 20,...100. This end time should be used inclusively"""
    times = []
    query1 = "select creation_date from post where owner_user_id = %(id)s and (post_type_id = 1 or post_type_id = 2) order by creation_date"
    cur.execute(query1, {'id': userID})
    posts = [i[0] for i in results(cur)]
    start = get_start_time(userID, cur)
    for p in sampling_percentiles:
        x = int(len(posts) * p)
        if x == 0:
            times.append(start)
        else:
            times.append(posts[x-1])
    return times   

def total_answers_helper(cur, start_time, end_time, userID):
    query = "select count(*) from post where owner_user_id = %(id)s and post_type_id = 2 and creation_date >= %(start_time)s and creation_date <= %(end_time)s"
    cur.execute(query, {'start_time': start_time, 'end_time': end_time, 'id': userID})
    return int([i[0] for i in results(cur)][0])

def total_answers(userID, cur):
    times = percentile_normalization(userID, cur, percentiles)
    result = []
    start_time = get_start_time(userID, cur)
    for time in times:
        result.append(total_answers_helper(cur, start_time, time, userID))
    return result

def total_accepted_answers_helper(cur, start_time, end_time, userID):
    query = "select count(x.id) from post x, post y where y.accepted_answer_id = x.id and x.owner_user_id = %(id)s and x.post_type_id = 2 and x.creation_date >= %(start_time)s and x.creation_date <= %(end_time)s"
    cur.execute(query, {'start_time': start_time, 'end_time': end_time, 'id': userID})
    return int([i[0] for i in results(cur)][0])

def total_accepted_answers(userID, cur):
    times = percentile_normalization(userID, cur, percentiles)
    result = []
    start_time = get_start_time(userID, cur)
    for time in times:
        result.append(total_accepted_answers_helper(cur, start_time, time, userID))
    return result

def get_indegree_at_time(cur, userID, time):
    graph = graph2.build_graph_before(cur, time)
    indegrees = graph2.indegree(graph)
    return indegrees[userID]

def get_betweenness_at_time(cur, userID, time):
    graph = graph2.build_graph_before_undirected(cur, time)
    betweenness = graph2.betweenness(graph)
    return betweenness[userID]

def get_closeness_at_time(cur, userID, time):
    graph = graph2.build_graph_before_undirected(cur, time)
    return graph2.closeness(graph, userID)

def get_pagerank_at_time(cur, userID, time):
    graph = graph2.build_graph_before(cur, time)
    ranks = graph2.pagerank(graph)
    return ranks[userID]

def get_auth_at_time(cur, userID, time):
    graph = graph2.build_graph_before(cur, time)
    ranks = graph2.hits(graph)
    return ranks[userID][1]

def get_cau_at_time(cur, conn, userID, time):
    return cau.cau(cur, conn, userID, time)

def get_elo_at_time(cur, conn, userID, time):
    return elo.elo(cur, conn, userID, time)

def cau_for_user(cur, conn, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_cau_at_time(cur, conn, userID, t) for t in times]

def elo_for_user(cur, conn, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_elo_at_time(cur, conn, userID, t) for t in times]

def pagerank_for_user(cur, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_pagerank_at_time(cur, userID, t) for t in times]

def auth_for_user(cur, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_auth_at_time(cur, userID, t) for t in times]

def indegree_for_user(cur, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_indegree_at_time(cur, userID, t) for t in times]

def betweenness_for_user(cur, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_betweenness_at_time(cur, userID, t) for t in times]

def closeness_for_user(cur, userID, samples = None):
    if not samples:
        samples = percentiles
    times = percentile_normalization(userID, cur, samples)
    return [get_closeness_at_time(cur, userID, t) for t in times]