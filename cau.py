#!/usr/bin/env python

import requests
from datetime import datetime
from search_utilities import *

def cau(cursor, conn, user_id, end_date = None):
    """
    Returns the CAU score for a given user. Optionally 
    consider only games played up until a given end date.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
    :param timebin: end_date for CAU calculation.
    """
    if not _cau_table_exists(cursor):
        _create_cau_table(cursor, conn, end_date)
    return _cau(cursor, user_id, end_date)

def cau_history(cursor, conn, user_id, end_date = None):
    """
    Returns a generator for the full CAU history for a given 
    user, across the time span of the dataset.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
    """
    if not _cau_table_exists(cursor):
        _create_cau_table(cursor, conn, end_date)
    return _cau_history(cursor, user_id)

####################################################
########### Private helper methods below ###########
####################################################

# Result are a tuple of
# (user id, user score, user answer date)
Result = namedtuple('Result', 'id score date')

# Tournaments are a tuple of
# (player 1 result, player 2 result, question id)
Tournament = namedtuple('Touernament', 'p1 p2 q_id')

def _cau_table_exists(cursor):
    """ 
    Checks if an cau rating table has been created.
    """
    statement = """SELECT EXISTS 
                      (SELECT 1
                       FROM   information_schema.tables 
                       WHERE  table_name = 'cau');
                """
    cursor.execute(statement)
    return cursor.fetchone()[0]

def _create_cau_table(cursor, connection, end_date = None):
    """
    Creates the cau table and computes cau information
    for all users over the complete time range of the
    dataset.
    """
    print "Building CAU table"
    statement = """CREATE TABLE IF NOT EXISTS cau (
                    foobarbaz bigserial PRIMARY KEY,
                    user_id bigint DEFAULT -1,
                    rating double precision DEFAULT 1500,
                    time timestamp DEFAULT NULL);
                """
    cursor.execute(statement)
    connection.commit()

    # Give default ratings for all players.
    users = _users_with_creation_date(cursor)
    for (user_id, creation_date) in users:
        _add_cau_with_date(connection.cursor(), user_id, 1500, creation_date)
    connection.commit()

    # Loop through all played games in playing order.
    for tournament in _users_by_tournament(cursor, end_date):
        # Unpack tournament details.
        p1_id = tournament.p1.id
        p1_score = tournament.p1.score
        p1_date = tournament.p1.date

        p2_id = tournament.p2.id
        p2_score = tournament.p2.score
        p2_date = tournament.p2.date

        # Get number of answers made to question.
        # (used to normalize scores)
        q_id = tournament.q_id
        replies = count_replies_to_post(connection.cursor(), q_id)
        normalizer = 1.0 / (replies - 1)
        
        # Compute tournament date (later of the two replies).
        tournament_date = max(p1_date, p2_date)

        # Fetch prior ratings for each player.
        p1_cau = _cau(connection.cursor(), p1_id)
        p2_cau = _cau(connection.cursor(), p2_id)

        # Calculate new CAU rating.
        p1_cau += normalizer * (p1_score - p2_score) 
        p2_cau += normalizer * (p2_score - p1_score)

        # Save new CAU ratings.
        _add_cau_with_date(connection.cursor(), p1_id, p1_cau, tournament_date)
        _add_cau_with_date(connection.cursor(), p2_id, p2_cau, tournament_date)
        
        connection.commit()
    print "Finished building CAU table"

def _add_cau_with_date(cursor, user_id, rating, date):
    """
    Adds a cau rating for a given user at a given
    date to the cau table.

    ** DOES NOT COMMIT THE TRANSACTION **
    ** THIS FASTER BULK LOADING WHERE NECESSARY **

    For use with CAU calculation.

    :param user_id: ID of the user to add rating for.
    :param rating: cau rating to add.
    :param time: the time associated with the rating.
    """
    statement = """INSERT INTO cau (user_id, rating, time)
                   VALUES (%(user_id)s, %(rating)s, %(date)s);
                """
    cursor.execute(statement, {"user_id": user_id, "rating": rating, "date": date})

def _cau_history(cursor, user_id):
    """
    Helper method to return cau history for a given
    user.

    ** ASSUMES CAU TABLE EXISTS **

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU history for
    """
    query = """SELECT rating, time
               FROM cau
               WHERE user_id = %(user_id)s
               ORDER BY time ASC;
            """
    cursor.execute(query, {"user_id": user_id})
    return [result for result in cursor]

def _cau(cursor, user_id, end_date = None):
    """
    Helper method to return the cau score for a given
    user. Optionally consider only games played
    up until a given end date.

    ** ASSUMES CAU TABLE EXISTS **

    For use with CAU calculation.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
    :param timebin: end_date for CAU calculation.
    """
    if end_date is None:
        query = """SELECT rating
                   FROM cau
                   WHERE user_id = %(user_id)s
                   ORDER BY time DESC
                   LIMIT 1;
                """
        cursor.execute(query, {"user_id": user_id})
    else:
        query = """SELECT rating
                   FROM cau
                   WHERE user_id = %(user_id)s
                   AND time <= %(date)s
                   ORDER BY time DESC
                   LIMIT 1;
                """
        cursor.execute(query, {"user_id": user_id, "date": end_date})
    return cursor.fetchone()[0]

def _users_with_creation_date(cursor):
    """
    Returns a generator for (user, creation_date)
    for all users.

    For use with CAU calculation.

    :param cursor: a Postgres database cursor
    """
    query = """SELECT bar.id, MIN(start) as start2 FROM (
                   SELECT DISTINCT u.id as id, LEAST(u.creation_date, p.creation_date) as start 
                   FROM se_user u 
                   LEFT OUTER JOIN Post p 
                   ON p.owner_user_id = u.id
               ) AS bar
               GROUP BY bar.id
               ORDER BY start2;"""
    cursor.execute(query)
    return (result for result in cursor)

def _users_by_tournament(cursor, end_date = None):
    """
    Returns a generator for 
    ((user_id, score), (user_id, score)) 
    pairs where both users post an answer the same 
    question.  Optionally considers only posts made 
    within a given time bin.

    Results are ordered by the date of the later
    post in the pair since that is when the
    tournament occurs.

    For use with CAU calculation.

    :param cursor: a Postgres database cursor
    :param timebin: a timebin to filter tournaments
    """
    if end_date is None:
        query = """SELECT u1.id, a1.score, a1.creation_date, 
                      u2.id, a2.score, a2.creation_date, 
                      q.id
                   FROM Post q
                   INNER JOIN Post a1
                   ON q.id = a1.parent_id
                   INNER JOIN Post a2
                   ON q.id = a2.parent_id
                   INNER JOIN se_user u1
                   ON u1.id = a1.owner_user_id
                   INNER JOIN se_user u2
                   ON u2.id = a2.owner_user_id
                   WHERE a1.id < a2.id
                   AND u1.id <> u2.id
                   ORDER BY GREATEST(a1.creation_date, a2.creation_date);
                """
        cursor.execute(query)
    else:
        query = """SELECT u1.id, a1.score, a1.creation_date, 
                      u2.id, a2.score, a2.creation_date, 
                      q.id
                   FROM Post q
                   INNER JOIN Post a1
                   ON q.id = a1.parent_id
                   INNER JOIN Post a2
                   ON q.id = a2.parent_id
                   INNER JOIN se_user u1
                   ON u1.id = a1.owner_user_id
                   INNER JOIN se_user u2
                   ON u2.id = a2.owner_user_id
                   WHERE a1.id < a2.id
                   AND u1.id <> u2.id
                   AND a1.creation_date < %(date)s
                   AND a2.creation_date < %(date)s
                   ORDER BY GREATEST(a1.creation_date, a2.creation_date);
                """
        cursor.execute(query, {"date": end_date})
    return (Tournament(Result(result[0], result[1], result[2]), 
                       Result(result[3], result[4], result[5]), 
                       result[6]) for result in cursor)
