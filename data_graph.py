#!/usr/bin/env python

import psycopg2
import snap
import sys
from datetime import date
from collections import Counter

DB_NAME = "stackexchangedb"
DB_USER = "postgres"

TIME_BINS = [
    (date(2012, 3, 1), date(2012, 9, 1)),
    (date(2012, 9, 1), date(2013, 3, 1)),
    (date(2013, 3, 1), date(2013, 9, 1)),
    (date(2013, 9, 1), date(2014, 3, 1)),
    (date(2014, 3, 1), date(2014, 9, 1)),
    (date(2014, 9, 1), date(2015, 3, 1)),
]


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


def add_nodes(cur, graph):
    """Add users to graph as nodes."""
    cur.execute("SELECT id FROM se_user;")
    for row in results(cur):
        user_id = row[0]

        # Filter out dummy users with ID < 0.
        if user_id < 0:
            continue
        graph.AddNode(user_id)


def add_nodes_before(cur, graph, cutoff):
    """Add users to graph as nodes."""
    cur.execute("SELECT id FROM se_user WHERE creation_date < %s;", (cutoff,))
    for row in results(cur):
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
               ON t1.parent_id = t2.id
               WHERE t1.post_type_id = 2 AND t2.post_type_id = 1;
            """

    cur.execute(query)
    for src, dst in results(cur):
        if src is None or dst is None:
            continue
        graph.AddEdge(src, dst)


def add_edges_time_slice(cur, graph, start_date, end_date):
    """Add a directed edge between each pair of nodes where the source
       user answered a question asked by the destination user.
    """

    query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.parent_id = t2.id
               WHERE t1.post_type_id = 2 AND t2.post_type_id = 1
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

    cur.execute(query, {'start_date': start_date, 'end_date': end_date})
    for src, dst in results(cur):
        if src is None or dst is None:
            continue
        graph.AddEdge(src, dst)


def add_edges_answer_question(cur, graph, start_date, end_date, directed, weighted, weights):
    """Add an edge between each pair of nodes where the source
       user answered a question asked by the destination user.
    """
    if not weighted:
        query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.parent_id = t2.id
               WHERE t1.post_type_id = 2 AND t2.post_type_id = 1
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """


        cur.execute(query, {'start_date': start_date, 'end_date': end_date})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)

    else:
        query = """SELECT t1.owner_user_id, t2.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.parent_id = t2.id
               WHERE t1.post_type_id = 2 AND t2.post_type_id = 1
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

        cur.execute(query, {'start_date': start_date, 'end_date': end_date})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)
            if directed:
                weights[(src, dst)] += 1
            else:
                if src < dest:
                    weights[(src, dst)] += 1
                else:
                    weights[(dst, src)] += 1


def add_edges_accepted_answer(cur, graph, start_date, end_date, directed, weighted, weights):
    """Add an edge between each pair of nodes where the source
       user has an accepted answer to a question asked by the destination user.
    """
    if not weighted:
        query = """SELECT DISTINCT t2.owner_user_id, t1.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.accepted_answer_id = t2.id
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """


        cur.execute(query, {'start_date': start_date, 'end_date': end_date})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)

    else:
        query = """SELECT t2.owner_user_id, t1.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.accepted_answer_id = t2.id
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

        cur.execute(query, {'start_date': start_date, 'end_date': end_date})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)
            if directed:
                weights[(src, dst)] += 1
            else:
                if src < dest:
                    weights[(src, dst)] += 1
                else:
                    weights[(dst, src)] += 1


def get_top_user_ids(cur, percentile=.1):
    """Get the ids of the top users ranked by reputation."""
    cur.execute("SELECT count(*) FROM se_user;")
    count = cur.fetchone()[0]
    limit = int(count * percentile)
    query = "SELECT id FROM se_user ORDER BY reputation DESC LIMIT %s"
    cur.execute(query, (limit,))
    return [i[0] for i in results(cur)]


def build_graph_time_slice(cur, start, end):
    graph = snap.TNGraph.New()
    add_nodes(cur, graph)
    add_edges_time_slice(cur, graph, start, end)
    return graph


def build_graph_answer_question(cur, start_date, end_date, directed=True, weighted=False):
    graph = snap.TNGraph.New()
    if not directed:
        graph = snap.TUNGraph.New()
    add_nodes(cur, graph)
    weights = Counter()
    add_edges_answer_question(cur, graph, start_date, end_date, directed, weighted, weights)
    return (graph, weights)


def build_graph_accepted_answer(cur, start_date, end_date, directed=True, weighted=False):
  graph = snap.TNGraph.New()
  if not directed:
      graph = snap.TUNGraph.New()
  add_nodes(cur, graph)
  weights = Counter()
  add_edges_accepted_answer(cur, graph, start_date, end_date, directed, weighted, weights)
  return (graph, weights)

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
