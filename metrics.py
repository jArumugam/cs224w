#!/usr/bin/env python

from __future__ import division
from collections import Counter
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

DB_NAME = "stackexchangedb"
DB_USER = "postgres"

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

def percentile_normalization(userID, cur):
    """returns a vector of end times for percentiles 0, 10, 20,...100. This end time should be used inclusively"""
    percentiles = [i*0.1 for i in range(0, 11)]
    times = []
    query1 = "select creation_date from post where owner_user_id = %(id)s and (post_type_id = 1 or post_type_id = 2) order by creation_date"
    cur.execute(query1, {'id': userID})
    posts = [i[0] for i in results(cur)]
    query2 = "select creation_date from se_user where id = %(id)s"
    cur.execute(query2, {'id': userID})
    start = [i[0] for i in results(cur)][0]
    for p in percentiles:
        x = int(len(posts) * p)
        print x
        if x == 0:
            times.append(start)
        else:
            times.append(posts[x-1])
    return times   


