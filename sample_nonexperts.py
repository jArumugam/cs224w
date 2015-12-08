#!/usr/bin/env python

import psycopg2
import numpy as np
import search_utilities

DB_NAME = "Ben-han"
DB_USER = "Ben-han"

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
    experts = tuple(search_utilities.get_experts())
    query1 = """SELECT u.id
                FROM se_user u
                INNER JOIN post p
                ON p.owner_user_id = u.id
                WHERE  u.id <> -1
                AND u.id NOT IN %(experts)s
                AND p.post_type_id = 2
                GROUP BY u.id HAVING Count(*) > 1
                ORDER BY random()
                LIMIT %(limit)s;"""
    cur.execute(query1, {"experts": experts, "limit": sample_size})
    return list(result[0] for result in cur)

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

