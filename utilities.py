from collections import namedtuple

# Time bins are a tuple (start, end) denoting
# the start and end dates of a timerange,
# respectively.
TimeBin = namedtuple('TimeBin', 'start end')

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

def users_in_post(cursor, post_id):
    """
    Returns a generator for IDs for users who 
    answered a post given by id.

    :param cursor: a Postgres database cursor
    :param post_id: the post id
    """
    query = """SELECT DISTINCT t1.owner_user_id
               FROM Post t1
               INNER JOIN Post t2
               ON t1.parent_id = t2.id
               WHERE t1.post_type_id = 2 
               AND t2.post_type_id = 1 
               AND t2.id = %(post_id)s;
            """
    cursor.execute(query, {'post_id': post_id})
    return (result[0] for result in cursor)