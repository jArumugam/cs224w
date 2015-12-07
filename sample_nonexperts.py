#!/usr/bin/env python

import psycopg2
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

def sample(cur, sample_size = 50):
    """Samples num users out of the non-expert population that has asked or answered at least one question (active)"""
    query1 = "select id from se_user where id <> -1"
    cur.execute(query1)
    users = [i[0] for i in results(cur)]
    experts = [683, 98, 755, 39, 9550, 31, 41, 157, 472, 8321]
    active_nonexperts = []
    for user in users:
        if user in experts:
            continue
        query2 = "select count(*) from post x, se_user y where owner_user_id = %(id)s and y.id = %(id)s and (post_type_id = 1 or post_type_id = 2) and reputation > 1"
        cur.execute(query2, {'id': user})
        num = int([i[0] for i in results(cur)][0])
        if num > 0: active_nonexperts.append(user)
    result = np.random.choice(active_nonexperts, sample_size, False)
    return list(result)

def view_reputations(sample, cur):
    reputations = []
    for user in sample:
        query = "select reputation from se_user where id = %(id)s"
        cur.execute(query, {'id': user})
        num = int([i[0] for i in results(cur)][0])
        reputations.append(num)
    return reputations

def main():
    conn, cur = connect()
    nonexperts = sample(cur)
    print nonexperts
    reputations = view_reputations(nonexperts, cur)
    print reputations

if __name__ == '__main__':
    main()

