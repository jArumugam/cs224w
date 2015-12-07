from collections import namedtuple

# Time bins are a tuple (start, end) denoting
# the start and end dates of a time range,
# respectively.
TimeBin = namedtuple('TimeBin', 'start end')

def posts_by_type(cursor, post_type = None):
    """
    Returns a generator of (post_id, creator_id) 
    for all posts of the given type.
    Types are as defined by StackOverflow schema.
    Types we're interested in are:
    1 - question
    2 - answer

    :param cursor: a Postgres database cursor
    :param timebin: the time bin
    """
    if post_type is None:
        post_type = 1
    query = """SELECT id, owner_user_id 
               FROM Post
               WHERE post_type_id = %(post_type)s;
            """
    cursor.execute(query, {'post_type': post_type})
    return ((result[0], result[1]) for result in cursor)

def posts_within_timebin(cursor, timebin):
    """
    Returns a generator for IDs for posts falling 
    within a time bin.

    :param cursor: a Postgres database cursor
    :param timebin: the time bin
    """
    query = """SELECT id
               FROM Post
               WHERE creation_date > %(start)s
               AND creation_date < %(end)s;
            """
    cursor.execute(query, {'start': timebin.start, 'end': timebin.end})
    return (result[0] for result in cursor)

def posts_by_user(cursor, user_id, timebin = None):
    """
    Returns a generator for IDs for posts made by a
    given user. Optionally filter by the given 
    timebin.

    :param cursor: a Postgres database cursor
    :param user_id: the user id
    :param timebin: timebin to filter posts.
    """
    if timebin is None:
        query = """SELECT id
                   FROM Post
                   WHERE owner_user_id = %(user_id)s;
                """
        cursor.execute(query, {'user_id': user_id})
    else:
        query = """SELECT id
                   FROM Post
                   WHERE owner_user_id = %(user_id)s
                   AND creation_date > %(start)s
                   AND creation_date < %(end)s;
                """
        cursor.execute(query, {'user_id': user_id, 'start': timebin.start, 'end': timebin.end})
    return (result[0] for result in cursor)

def users_above_threshold(cursor, threshold):
    """
    Returns a generator for IDs for users with 
    reputation above a threshold.

    :param cursor: a Postgres database cursor
    :param threshold: the reputation threshold
    """
    query = """SELECT id
               FROM se_user
               WHERE reputation > %(threshold)s;
            """
    cursor.execute(query, {'threshold': threshold})
    return (result[0] for result in cursor)

def users_in_post(cursor, post_id, timebin = None):
    """
    Returns a generator for IDs for users who 
    answered a post given by id. Optionally filter
    by users who answered within a given timebin.

    :param cursor: a Postgres database cursor
    :param post_id: the post id
    """
    if timebin is None:
        query = """SELECT DISTINCT t1.owner_user_id
                   FROM Post t1
                   INNER JOIN Post t2
                   ON t1.parent_id = t2.id
                   WHERE t1.post_type_id = 2 
                   AND t2.post_type_id = 1 
                   AND t2.id = %(post_id)s;
                """
        cursor.execute(query, {'post_id': post_id})
    else:
        query = """SELECT DISTINCT t1.owner_user_id
                   FROM Post t1
                   INNER JOIN Post t2
                   ON t1.parent_id = t2.id
                   WHERE t1.post_type_id = 2 
                   AND t2.post_type_id = 1 
                   AND t2.id = %(post_id)s
                   AND t1.creation_date > %(start)s
                   AND t1.creation_date < %(end)s;
                """
        cursor.execute(query, {'post_id': post_id, 'start': timebin.start, 'end': timebin.end})
    return (result[0] for result in cursor)

def elo(cursor, user_id, end_date = None):
    """
    Returns the elo score for a given user. Optionally 
    consider only games played up until a given end date.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
    :param timebin: end_date for ELO calculation.
    """
    if not _elo_table_exists(cur):
        _create_elo_table(cur)
    return _elo(cursor, user_id, end_date)

def cau(cursor, user_id, timebin = None):
    """
    Returns the cumulative aggregate upvote (CAU) score for
    a user. By default this returns the score over the
    entire dataset. Optionally specify a timebin to
    compute the cau score within a specific time interval.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
    :param timebin: timebin to restrict CAU computation.
    """
    if timebin is None:
        query = """SELECT SUM(score) FROM Post
                   WHERE owner_user_id = %(user_id)s
                   AND post_type_id = 2
                """
        cursor.execute(query, {'user_id': user_id})
    else:
        query = """SELECT SUM(score) FROM Post
                   WHERE owner_user_id = %(user_id)s
                   AND post_type_id = 2
                   AND creation_date > %(start)s
                   AND creation_date < %(end)s;
                """
        cursor.execute(query, {'user_id': post_id, 'start': timebin.start, 'end': timebin.end})
    return cur.fetchone()

####################################################
########### Private helper methods below ###########
####################################################

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

def _create_elo_table(cursor, connection):
    """
    Creates the elo table and computes elo information
    for all users over the complete time range of the
    dataset.
    """
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

    # Loop through all played games in playing order
    for result in _users_by_tournament(cursor):
        # Unpack tournament details.
        p1_details = result[0]
        p1_id = p1_details[0]
        p1_score = p1_details[1]
        p1_date = p1_details[2]

        p2_details = result[1]
        p2_id = p2_details[0]
        p2_score = p2_details[1]
        p2_date = p2_details[2]

        # Compute tournament date (later of the two replies)
        tournament_date = max(p1_date, p2_date)

        # Fetch prior ratings for each player.
        p1_elo = _elo(connection.cursor(), p1_id)
        p2_elo = _elo(connection.cursor(), p2_id)

        # Update according to winner of tournament
        if p1_score == p2_score:
            p1_elo += 0.5
            p2_elo += 0.5
        elif p1_score > p2_score:
            p1_elo += 1
            p2_elo -= 0.5
        else:
            p1_elo -= 0.5
            p2_elo += 1
        _add_elo_with_date(connection.cursor(), p1_id, p1_elo, tournament_date)
        _add_elo_with_date(connection.cursor(), p2_id, p2_elo, tournament_date)
        
        connection.commit()

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

def _elo(cursor, user_id, end_date = None):
    """
    Helper method to return the elo score for a given
    user. Optionally consider only games played
    up until a given end date.

    ** ASSUMES ELO TABLE EXISTS **

    For use with ELo calculation.

    :param cursor: a Postgres database cursor
    :param user_id: ID of user you want the CAU score for
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

def _users_by_tournament(cursor, timebin = None):
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
    if timebin is None:
        query = """SELECT u1.id, a1.score, u2.id, a2.score, a1.creation_date, a2.creation_date
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
        query = """SELECT u1.id, a1.score, u2.id, a2.score, a1.creation_date, a2.creation_date
                   FROM Post q
                   INNER JOIN Post a1
                   ON q.id = a1.parent_id
                   INNER JOIN Post a2
                   ON q.id = a2.parent_id
                   INNER JOIN se_user u1
                   ON u1.id = a1.owner_user_id
                   INNER JOIN se_user u2
                   ON u2.id = a2.owner_user_id
                   WHERE a1.creation_date > %(start)s
                   AND a1.creation_date < %(end)s
                   AND a2.creation_date > %(start)s
                   AND a2.creation_date < %(end)s
                   AND a1.id < a2.id
                   AND u1.id <> u2.id
                   ORDER BY GREATEST(a1.creation_date, a2.creation_date);
                """
        cursor.execute(query, {'start': timebin.start, 'end': timebin.end})
    return (((result[0], result[1], result[4]), (result[2], result[3], result[5])) for result in cursor)
