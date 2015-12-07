#!/usr/bin/env python

from collections import namedtuple
from datetime import date, datetime

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

def count_replies_to_post(cursor, post_id):
    """
    Returns a count of the number of replies made
    to a given post identified by ID.

    :param cursor: a Postgres database cursor
    :param post_id: the post id
    """
    query = """SELECT COUNT(*)
               FROM Post
               WHERE parent_id = %(post_id)s;
            """
    cursor.execute(query, {'post_id': post_id})
    return cursor.fetchone()[0]

def count_posts_by_user(cursor, user_id, end_date = None):
    """
    Returns a count of posts made by a given user
    up until the end_date or the end of the dataset
    if an end_date is not provided.

    :param cursor: a Postgres database cursor
    :param user_id: the user id
    :param end_date: end date for posts
    """
    if end_date is None:
        query = """SELECT COUNT(*)
                   FROM Post
                   WHERE owner_user_id = %(user_id)s;
                """
        cursor.execute(query, {'user_id': user_id})
    else:
        query = """SELECT COUNT(*)
                   FROM Post
                   WHERE owner_user_id = %(user_id)s
                   AND creation_date < %(end)s;
                """
        cursor.execute(query, {'user_id': user_id, 'end': end_date})
    return cursor.fetchone()[0]

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

def get_top_users_by_percentile(cursor, percentile=.1):
    """Get the usernames and reputations of the top users ranked by reputation by percentile."""
    cursor.execute("SELECT count(*) FROM se_user;")
    count = cursor.fetchone()[0]
    limit = int(count * percentile)
    query = "SELECT display_name, reputation FROM se_user ORDER BY reputation DESC LIMIT %s"
    cursor.execute(query, (limit,))
    return [(i[0], i[1]) for i in cursor]

def get_top_users_by_num(cursor, num = 10):
    """Get the usernames and reputations of the top users ranked by reputation by number."""
    query = "SELECT display_name, reputation FROM se_user ORDER BY reputation DESC LIMIT %s"
    cursor.execute(query, (num,))
    return [(i[0], i[1]) for i in cursor]

def get_experts():
    return [683, 98, 755, 39, 9550, 31, 41, 157, 472, 8321]

def get_nonexperts():
    return [22825, 26997, 10235, 20922, 17953, 4766, 1280, 1820, 24256, 6518, 12901, 1362, 19347, 16190, 24037, 14379, 21365, 4421, 10086, 7934, 15249, 600, 20898, 17031, 26450, 12933, 25864, 2406, 1655, 8551, 7432, 16709, 17338, 9667, 4631, 17999, 19809, 25909, 11412, 10252, 18102, 14466, 4864, 9565, 7974, 9994, 9741, 4547, 25831, 24188]