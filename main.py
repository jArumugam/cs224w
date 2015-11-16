#!/usr/bin/env python

"""
Example code for working with the database.

psycopg2 documentation: http://initd.org/psycopg/docs/index.html

"""

import psycopg2
import snap
import sys

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


def main(argv):
    if len(argv) < 2:
        print "Usage: {} limit".format(argv[0])
        return

    limit = argv[1]

    conn, cur = connect()
    cur.execute("SELECT * FROM Post LIMIT %s", (limit,))

    for result in results(cur):
        print result


if __name__ == '__main__':
    main(sys.argv)
