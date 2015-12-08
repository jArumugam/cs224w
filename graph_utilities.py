#!/usr/bin/env python

import search_utilities
import snap

def qa_graph(cursor, directed = True, timebin = None):
    """
    Returns a SNAP graph of users where user A is
    connected to user B if user B answered a question
    made by user A. Optionally considers only the
    answers made within a given timebin.

    :param cursor: a Postgres database cursor
    :param directed: whether the graph should be directed 
    :param timebin: a timebin to filter answers
    """
    if directed:
        graph = snap.TNGraph.New()
    else:
        graph = snap.TUNGraph.New()

    # Add nodes
    users = search_utilities.users_above_threshold(cursor, 0)
    for user_id in users:
        # Filter out dummy users with ID < 0.
        if user_id < 0:
            continue
        graph.AddNode(user_id)

    # Build edges
    pairs = search_utilities.asker_answerer_pairs(cursor, timebin)
    for (asker, answerer) in pairs:
        if not asker or not answerer:
            continue
        graph.AddEdge(asker, answerer)

    return graph

