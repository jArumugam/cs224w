#!/usr/bin/env python

from __future__ import division
from collections import Counter
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

DB_NAME = "kulshrax"
DB_USER = "kulshrax"

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

def get_reputation(cur):
    query = "SELECT reputation FROM se_user;"
    cur.execute(query)
    return [i[0] for i in results(cur)]
        

def main():
    conn, cur = connect()
    reputation = get_reputation(cur)

    counts = Counter(reputation)

    plt.xscale('log')
    plt.yscale('log')
    # Logarithmic binning
    bins = np.logspace(1, np.log10(60000))
    plt.hist(reputation, bins, log=True)

    plt.show()



if __name__ == '__main__':
    main()