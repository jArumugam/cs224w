#!/usr/bin/env python

import psycopg2
import snap
import sys
import elo
import matplotlib.pyplot as plt
from datetime import date
from collections import Counter

DB_NAME = "Ben-han"
DB_USER = "Ben-han"

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

def compute_user_reputations(cur, conn):
    """Computes time-based user reputation as the number of upvotes
       gained from posts made before a certain time. Calculates a user's
       cumulative reputation according to this metric for each time bin
       above and saves it to the |upvotes| table."""

    reputation_query = """SELECT SUM(score) FROM Post
                          WHERE owner_user_id = %(user_id)s
                          AND creation_date > %(start_date)s
                          AND creation_date < %(end_date)s;
                       """
    insertion_query = """INSERT INTO upvotes VALUES (%(user_id)s, %(r1)s, %(r2)s, %(r3)s, %(r4)s, %(r5)s, %(r6)s);
                      """

    cur.execute("SELECT id FROM se_user;")
    user_ids = list(cur)
    for user_id in user_ids:
        # Skip dummy users.
        if user_id < 0:
            continue

        # Calculate user rep per time bin.
        rep = []
        for bin in TIME_BINS:
            cur.execute(reputation_query, {'user_id': user_id, 'start_date': bin[0], 'end_date': bin[1]})
            rep.append(cur.fetchone())

        # Insert into 'upvotes' table.
        cur.execute(insertion_query, {'user_id': user_id, 'r1': rep[0], 'r2': rep[1], 'r3': rep[2], 'r4': rep[3], 'r5': rep[4], 'r6': rep[5]})
        conn.commit()

def foobarbaz(data, start_date, end_date):
    """Extracts ELO data from within a start_date and
       end_date and returns the latest date. Do not question
       the name of this function."""
    dates = [entry for entry in data if start_date < entry[0] <= end_date]
    if len(dates) > 0:
        return max(dates)
    return 1500 # default ELO rating

def compute_elo_ratings(cur, conn):
    """Computes the user's elo rating for each time bin
       above and saves it to the |elo| table. JK about computing...
       we're just scraping it from a website. See 
       http://stackrating.com/"""

    insertion_query = """INSERT INTO elo VALUES (%(user_id)s, %(r1)s, %(r2)s, %(r3)s, %(r4)s, %(r5)s, %(r6)s);
                      """

    cur.execute("SELECT id FROM se_user;")
    user_ids = list(cur)
    for user_id in user_ids:
        # Skip dummy users.
        if user_id < 0:
            continue

        # Fetch user elo ratings
        data = elo.get_elo_data(user_id)

        # Calculate ELO rep per time bin.
        rep = [foobarbaz(data, bin[0], bin[1]) for bin in TIME_BINS]

        # Insert into 'elo' table.
        cur.execute(insertion_query, {'user_id': user_id, 'r1': rep[0], 'r2': rep[1], 'r3': rep[2], 'r4': rep[3], 'r5': rep[4], 'r6': rep[5]})
        conn.commit()

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
               INNER JOIN se_user u1
               ON t1.owner_user_id = u1.id
               INNER JOIN se_user u2
               ON t2.owner_user_id = u2.id
               WHERE t1.post_type_id = 2 AND t2.post_type_id = 1
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s
               AND u1.reputation < u2.reputation;
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

def add_edges_answer_question_above_threshold(cur, graph, start_date, end_date, threshold, directed, weighted, weights):
    """Add an edge between each pair of nodes where the source
       user answered a question asked by the destination user 
       and the desgination user is above a certain threshold.
    """
    if not weighted:
        query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1, Post t2, se_user u
               WHERE t1.parent_id = t2.id AND t2.owner_user_id = u.id
               AND t1.post_type_id = 2 AND t2.post_type_id = 1 AND u.reputation > %(threshold)s
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """
        cur.execute(query, {'start_date': start_date, 'end_date': end_date, 'threshold': threshold})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)

    else:
        query = """SELECT t1.owner_user_id, t2.owner_user_id
               FROM Post t1, Post t2, se_user u
               WHERE t1.parent_id = t2.id AND t2.owner_user_id = u.id
               AND t1.post_type_id = 2 AND t2.post_type_id = 1 AND u.reputation > %(threshold)s
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

        cur.execute(query, {'start_date': start_date, 'end_date': end_date, 'threshold': threshold})
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

def add_edges_answer_question_below_threshold(cur, graph, start_date, end_date, threshold, directed, weighted, weights):
    """Add an edge between each pair of nodes where the source
       user answered a question asked by the destination user 
       and the desgination user is below a certain threshold.
    """
    if not weighted:
        query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
               FROM Post t1, Post t2, se_user u
               WHERE t1.parent_id = t2.id AND t2.owner_user_id = u.id
               AND t1.post_type_id = 2 AND t2.post_type_id = 1 AND u.reputation < %(threshold)s
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

        cur.execute(query, {'start_date': start_date, 'end_date': end_date, 'threshold': threshold})
        for src, dst in results(cur):
            if src is None or dst is None:
                continue
            graph.AddEdge(src, dst)

    else:
        query = """SELECT t1.owner_user_id, t2.owner_user_id
               FROM Post t1, Post t2, se_user u
               WHERE t1.parent_id = t2.id AND t2.owner_user_id = u.id
               AND t1.post_type_id = 2 AND t2.post_type_id = 1 AND u.reputation < %(threshold)s
               AND t1.creation_date > %(start_date)s
               AND t1.creation_date < %(end_date)s
               AND t2.creation_date > %(start_date)s
               AND t2.creation_Date < %(end_date)s;
            """

        cur.execute(query, {'start_date': start_date, 'end_date': end_date, 'threshold': threshold})
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

def build_graph_answer_question_above_threshold(cur, start_date, end_date, threshold, directed=True, weighted=False):
  graph = snap.TNGraph.New()
  if not directed:
      graph = snap.TUNGraph.New()
  add_nodes(cur, graph)
  weights = Counter()
  add_edges_answer_question_above_threshold(cur, graph, start_date, end_date, threshold, directed, weighted, weights)
  return (graph, weights)

def build_graph_answer_question_below_threshold(cur, start_date, end_date, threshold, directed=True, weighted=False):
  graph = snap.TNGraph.New()
  if not directed:
      graph = snap.TUNGraph.New()
  add_nodes(cur, graph)
  weights = Counter()
  add_edges_answer_question_below_threshold(cur, graph, start_date, end_date, threshold, directed, weighted, weights)
  return (graph, weights)

def main(argv):
    # Connect to DB.
    db_name = DB_NAME
    db_user = DB_USER
    if len(argv) > 3:
        db_name = argv[1]
        db_user = argv[2]
    conn, cur = connect(db_name, db_user)

    # Identify nodes we're interested in.
    cur.execute("SELECT u1.id FROM se_user u1 INNER JOIN upvotes u2 ON u1.id = u2.id WHERE u2.bin1 + u2.bin2 + u2.bin3 + u2.bin4 + u2.bin5 + u2.bin6 > 1600;")
    user_ids = [user_id[0] for user_id in cur if user_id[0] != -1]
    avg_out_degs = []

    # Build a graph per time slice.
    for bin in TIME_BINS:
        graph = build_graph_time_slice(cur, bin[0], bin[1])
        print "Nodes in graph:", graph.GetNodes()
        print "Edges in graph:", graph.GetEdges()

        # Record per-user outdegree at that time slice.
        avg_out_deg = 0.0
        for user_id in user_ids:
            out_deg = graph.GetNI(user_id).GetOutDeg()
            avg_out_deg = avg_out_deg + float(out_deg) / len(user_ids)
        avg_out_degs.append(avg_out_deg)

    # Plot average user out-degree at each time slice.
    x = range(0, len(avg_out_degs))
    plt.plot(x, avg_out_degs)
    plt.savefig("test")
    plt.show()


if __name__ == '__main__':
    main(sys.argv)
