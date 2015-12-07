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

def get_posts_distribution(cur):
    query1 = "select id from se_user where id <> -1"
    cur.execute(query1)
    users = [i[0] for i in results(cur)]
    distribution_questions = Counter()
    distribution_answers = Counter()
    for user in users:
        query2 = "select count(*) from post where owner_user_id = %(id)s and post_type_id = 1"
        cur.execute(query2, {'id': user})
        num = int([i[0] for i in results(cur)][0])
        distribution_questions[num] += 1
        query3 = "select count(*) from post where owner_user_id = %(id)s and post_type_id = 2"
        cur.execute(query3, {'id': user})
        num = int([i[0] for i in results(cur)][0])
        distribution_answers[num] += 1
    return (distribution_questions, distribution_answers)



        

def main():
    conn, cur = connect()
    questions, answers = get_posts_distribution(cur)
    print questions
    print answers


if __name__ == '__main__':
    main()