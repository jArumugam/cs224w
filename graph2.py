#!/usr/bin/env python

import psycopg2
import snap
import sys
from datetime import date
from collections import Counter
from dateutil.parser import parse

DB_NAME = "stackexchange"
DB_USER = "kulshrax"


def connect(db=DB_NAME, user=DB_USER):
    """Connect to the specified Postgres database as the specified user."""
    conn = psycopg2.connect("dbname={} user={}".format(db, user))
    cur = conn.cursor()
    return conn, cur


def add_nodes(cur, graph):
    """Add users to graph as nodes."""
    cur.execute("SELECT id FROM se_user;")
    for row in cur:
        user_id = row[0]

        # Filter out dummy users with ID < 0.
        if user_id < 0:
            continue
        graph.AddNode(user_id)


def add_nodes_before(cur, graph, cutoff=None):
    """Add users to graph as nodes."""
    cur.execute("SELECT id FROM se_user WHERE creation_date < %s;", (cutoff,))
    for row in cur:
        user_id = row[0]

        # Filter out dummy users with ID < 0.
        if user_id < 0:
            continue
        graph.AddNode(user_id)


def add_edges(cur, graph):
    """Add a directed edge between each pair of nodes where the source
       user answered a question asked by the destination user.
    """

    query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.id = t2.parent_id
               WHERE t1.post_type_id = 1 AND t2.post_type_id = 2;
            """

    cur.execute(query)
    for src, dst in cur:
        if src is None or dst is None or src < 0 or dst < 0:
            continue
        graph.AddEdge(src, dst)


def add_edges_before(cur, graph, cutoff):
    """Add a directed edge between each pair of nodes where the source
       user answered a question asked by the destination user.
    """

    query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.id = t2.parent_id
               WHERE t1.post_type_id = 1 AND t2.post_type_id = 2;
               AND t1.creation_date < %(cutoff)s
               AND t2.creation_date < %(cutoff)s;
            """

    cur.execute(query, {'cutoff': cutoff})
    for src, dst in cur:
        if src is None or dst is None:
            continue
        graph.AddEdge(src, dst)


def build_graph(cur):
    graph = snap.TNGraph.New()
    add_nodes(cur, graph)
    add_edges(cur, graph)
    return graph


def build_graph_before(cur, cutoff):
    if type(cutoff) == 'str':
        cutoff = parse(cutoff)
    graph = snap.TNGraph.New()
    add_nodes_before(cur, graph, cutoff)
    add_edges_before(cur, graph, cutoff)
    return graph


def main(argv):
    db_name = DB_NAME
    db_user = DB_USER
    if len(argv) > 3:
        db_name = argv[1]
        db_user = argv[2]

    conn, cur = connect(db_name, db_user)
    graph = build_graph(cur)
    
    print "Nodes in graph:", graph.GetNodes()
    print "Edges in graph:", graph.GetEdges()


if __name__ == '__main__':
    main(sys.argv)