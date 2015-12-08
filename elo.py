#!/usr/bin/env python

import requests
from datetime import datetime
from search_utilities import *

def elo(cursor, conn, user_id, end_date = None):
    """
    Returns the ELO score for a given user. Optionally 
    consider only games played up until a given end date.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the ELO score for
    :param timebin: end_date for ELO calculation.
    """
    if not _elo_table_exists(cursor):
        _create_elo_table(cursor, conn, end_date)
    return _elo(cursor, user_id, end_date)

def elo_history(cursor, conn, user_id, end_date = None):
    """
    Returns a generator for the full ELO history for a given 
    user, across the time span of the dataset.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the ELO score for
    """
    if not _elo_table_exists(cursor):
        _create_elo_table(cursor, conn, end_date)
    return _elo_history(cursor, user_id)

####################################################
########### Private helper methods below ###########
####################################################

# Result are a tuple of
# (user id, user score, user answer date)
Result = namedtuple('Result', 'id score date')

# Tournaments are a tuple of
# (player 1 result, player 2 result, question id)
Tournament = namedtuple('Touernament', 'p1 p2 q_id')

def _elo_table_exists(cursor):
    """ 
    Checks if an elo rating table has been created.
    """
    statement = """SELECT EXISTS 
                      (SELECT 1
                       FROM   information_schema.tables 
                       WHERE  table_name = 'elo');
                """
    cursor.execute(statement)
    return cursor.fetchone()[0]

def _create_elo_table(cursor, connection, end_date = None):
    """
    Creates the elo table and computes elo information
    for all users over the complete time range of the
    dataset.
    """
    print "Building ELO table"
    statement = """CREATE TABLE IF NOT EXISTS elo (
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
        _add_elo_with_date(connection.cursor(), user_id, 1500, creation_date)
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
        p1_elo = _elo(connection.cursor(), p1_id)
        p2_elo = _elo(connection.cursor(), p2_id)

        # Computed expected results.
        # Expected to win ==> Want big positive difference rating.
        p1_expected_result = 1.0 / (10 ** (-(p1_elo - p2_elo) / 400.0) + 1)
        p2_expected_result = 1.0 / (10 ** (-(p2_elo - p1_elo) / 400.0) + 1)

        # Compute weight constant K for each player.
        p1_games_played = count_posts_by_user(connection.cursor(), p1_id, tournament_date.date())
        p2_games_played = count_posts_by_user(connection.cursor(), p2_id, tournament_date.date())

        threshold = 100.0
        K1 = 8 if p1_games_played < threshold else (1 if p2_games_played < threshold else 4)
        K2 = 8 if p2_games_played < threshold else (1 if p1_games_played < threshold else 4)

        # Update ELO ratings according to winner of tournament
        # and expected outcome.
        if p1_score == p2_score:
            # Draw. Score is +0.5 for each player.
            p1_update = 0.5 - p1_expected_result
            p2_update = 0.5 - p2_expected_result
        elif p1_score > p2_score:
            # P1 wins. Score is +1 for P1 and an interpolated value
            # between +0 and +0.5 for P2.
            p1_score += 0.00001
            p1_update = 1 - p1_expected_result
            p2_update = max((p2_score - 0.5 * p1_score) / (p1_score * 0.5), 0) * 0.5 - p2_expected_result
        else:
            # P2 wins. Score is +1 for P2 and an interpolated value
            # between +0 and +0.5 for P1.
            p2_score += 0.00001
            p1_update = max((p1_score - 0.5 * p2_score) / (p2_score * 0.5), 0) * 0.5 - p1_expected_result
            p2_update = 1 - p2_expected_result
        p1_elo += normalizer * K1 * p1_update 
        p2_elo += normalizer * K2 * p2_update

        # Save new ELO ratings.
        _add_elo_with_date(connection.cursor(), p1_id, p1_elo, tournament_date)
        _add_elo_with_date(connection.cursor(), p2_id, p2_elo, tournament_date)
        
        connection.commit()
    print "Finished building ELO table"

def _add_elo_with_date(cursor, user_id, rating, date):
    """
    Adds a elo rating for a given user at a given
    date to the elo table.

    ** DOES NOT COMMIT THE TRANSACTION **
    ** THIS FASTER BULK LOADING WHERE NECESSARY **

    For use with ELO calculation.

    :param user_id: ID of the user to add rating for.
    :param rating: elo rating to add.
    :param time: the time associated with the rating.
    """
    statement = """INSERT INTO elo (user_id, rating, time)
                   VALUES (%(user_id)s, %(rating)s, %(date)s);
                """
    cursor.execute(statement, {"user_id": user_id, "rating": rating, "date": date})

def _elo_history(cursor, user_id):
    """
    Helper method to return elo history for a given
    user.

    ** ASSUMES ELO TABLE EXISTS **

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the ELO history for
    """
    query = """SELECT rating, time
               FROM elo
               WHERE user_id = %(user_id)s
               ORDER BY time ASC;
            """
    cursor.execute(query, {"user_id": user_id})
    return [result for result in cursor]

def _elo(cursor, user_id, end_date = None):
    """
    Helper method to return the elo score for a given
    user. Optionally consider only games played
    up until a given end date.

    ** ASSUMES ELO TABLE EXISTS **

    For use with ELO calculation.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the ELO score for
    :param timebin: end_date for ELO calculation.
    """
    if end_date is None:
        query = """SELECT rating
                   FROM elo
                   WHERE user_id = %(user_id)s
                   ORDER BY time DESC
                   LIMIT 1;
                """
        cursor.execute(query, {"user_id": user_id})
    else:
        query = """SELECT rating
                   FROM elo
                   WHERE user_id = %(user_id)s
                   AND time < %(date)s
                   ORDER BY time DESC
                   LIMIT 1;
                """
        cursor.execute(query, {"user_id": user_id, "date": end_date})
    return cursor.fetchone()[0]

def _users_with_creation_date(cursor):
    """
    Returns a generator for (user, creation_date)
    for all users.

    For use with ELO calculation.

    :param cursor: a Postgres database cursor
    """
    query = """SELECT id, creation_date
               FROM se_user;
            """
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

    For use with ELO calculation.

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
