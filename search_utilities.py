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

def count_users_by_reputation(cursor):
    """
    Returns a generator of of tuples (reputation, user count) 
    indicating a reputation score and the number of users with 
    that reputation.

    :param cursor: a Postgres database cursor
    """
    query = """SELECT reputation, Count(*) 
               FROM se_user 
               GROUP BY reputation
               ORDER BY reputation DESC
               LIMIT 720;
            """
    cursor.execute(query)
    return ((result[0], result[1]) for result in cursor)

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

def asker_answerer_pairs(cursor, timebin = None):
    """
    Returns a generator for (asker, answerer) pairs
    where answerer is a user who answers a question
    asked by asker. Optionally restricts results
    to a provided timebin.
    """
    if timebin is None:
        query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
                   FROM Post t1
                   INNER JOIN Post t2
                   ON t2.parent_id = t1.id
                   WHERE t1.post_type_id = 1 AND t2.post_type_id = 2;
                """
        cursor.execute(query)
    else:
        query = """SELECT DISTINCT t1.owner_user_id, t2.owner_user_id
                   FROM Post t1
                   INNER JOIN Post t2
                   ON t2.parent_id = t1.id
                   WHERE t1.post_type_id = 1 AND t2.post_type_id = 2
                   AND t1.creation_date > %(start)s
                   AND t1.creation_date < %(end)s
                   AND t2.creation_date > %(start)s
                   AND t2.creation_Date < %(end)s;
                """
        cursor.execute(query, {'start': timebin.start, 'end': timebin.end})
    return ((result[0], result[1]) for result in cursor)

def get_top_users_by_percentile(cursor, percentile=.1):
    """
    Returns the ids of the top |percentile| of users after 
    ordering by reputation.
    """
    cursor.execute("SELECT count(*) FROM se_user;")
    count = cursor.fetchone()[0]
    limit = int(count * percentile)
    query = "SELECT display_name, id FROM se_user ORDER BY reputation DESC LIMIT %s"
    cursor.execute(query, (limit,))
    return [i[1] for i in cursor]

def get_top_users_by_num(cursor, num = 10):
    """Get the usernames and reputations of the top users ranked by reputation by number."""
    query = "SELECT display_name, reputation FROM se_user ORDER BY reputation DESC LIMIT %s"
    cursor.execute(query, (num,))
    return [(i[0], i[1]) for i in cursor]

def get_experts():
    # CS Stack Exchange: return [683, 98, 755, 39, 9550, 31, 41, 157, 472, 8321]

    # Cooking Exchange short version - For graph generation, etc.
    # return [14401, 4638, 41, 67, 60, 20183, 4194, 2001, 1672, 1393, 3203, 426]

    # Cooking Exchange long version - This is the top 1%. For training data.
    return [14401, 4638, 41, 67, 60, 20183, 4194, 2001, 1672, 1393, 3203, 426, 
            6345, 19707, 1259, 218, 1816, 1374, 7180, 183, 160, 6279, 15, 641, 4580, 
            3348, 231, 210, 364, 446, 3649, 18453, 611, 1236, 2047, 2909, 115, 15018, 
            1887, 5455, 25059, 649, 304, 10685, 5600, 624, 86, 6317, 3630, 1443, 26180, 
            1685, 220, 2832, 1148, 123, 403, 3479, 1229, 8305, 1549, 4489, 6808, 1415, 
            9091, 4504, 28879, 125, 5505, 266, 6531, 1163, 26816, 43, 17143, 3432, 
            14601, 16, 14096, 8522, 27, 1571, 2391, 2569, 8920, 8457, 104, 3234, 10218, 
            5885, 4214, 8158, 8315, 87, 6615, 6638, 7552, 197, 4442, 10898, 3819, 19206, 
            6168, 4535, 14539, 2239, 6127, 982, 2010, 1670, 201, 11200, 7092, 3756, 
            2690, 12565, 4777, 1675, 373, 296, 33, 27244, 3779, 152, 8499, 45, 2125, 99, 
            1601, 1832, 990, 91, 4012, 219, 4558, 358, 177, 47, 190, 10201, 178, 7632, 
            51, 725, 2951, 835, 17063, 9799, 126, 8339, 8766, 2882, 9453, 5660, 6320, 
            3489, 24248, 164, 438, 6818, 770, 13972, 20, 4853, 812, 6498, 28723, 3852, 
            2065, 7082, 4593, 149, 557, 32, 2283, 20118, 1759, 4428, 23376, 215, 145, 
            7060, 3345, 10942, 20069, 207, 18159, 4341, 5646, 539, 8434, 689, 1546, 4817, 
            114, 30873, 4303, 6791, 4590, 61, 10642, 19, 5263, 2939, 3342, 22, 4414, 
            4047, 9057, 8, 4039, 809, 17, 243, 11556, 9401, 9262, 1230, 203, 25286]

def get_nonexperts():
    # CS Stack Exchange: return [22825, 26997, 10235, 20922, 17953, 4766, 1280, 1820, 24256, 6518, 12901, 1362, 19347, 16190, 24037, 14379, 21365, 4421, 10086, 7934, 15249, 600, 20898, 17031, 26450, 12933, 25864, 2406, 1655, 8551, 7432, 16709, 17338, 9667, 4631, 17999, 19809, 25909, 11412, 10252, 18102, 14466, 4864, 9565, 7974, 9994, 9741, 4547, 25831, 24188]

    # Cooking Exchange short version.
    # return [1687, 11077, 14978, 22764, 7574, 2805, 459, 26186, 8582, 6809, 29789]

    # Cooking Exchange long version.
    return [1687, 11077, 14978, 22764, 7574, 2805, 459, 26186, 8582, 6809, 29789, 
            21575, 7348, 22597, 2591, 10078, 688, 2215, 12608, 31388, 9935, 1798, 5212, 
            24641, 27093, 7439, 27097, 18762, 19842, 2289, 33210, 10687, 3832, 27288, 184, 
            1859, 3418, 10033, 9594, 22275, 1276, 18133, 18723, 17055, 14621, 27590, 6533, 
            72, 8847, 5865, 23733, 4873, 2959, 3158, 652, 24678, 615, 26099, 9165, 65, 
            10250, 5524, 33365, 24724, 18343, 5114, 5130, 137, 20057, 14349, 20707, 18910, 
            22441, 8327, 7561, 716, 23177, 2052, 7107, 26443, 5781, 4547, 6288, 5389, 
            20285, 519, 15045, 24017, 11082, 260, 4226, 8136, 18028, 2389, 9897, 6499, 
            12591, 23256, 1509, 267, 5471, 7000, 94, 1434, 7846, 1502, 14467, 15395, 
            33883, 11332, 25838, 2291, 14207, 17363, 5962, 8675, 143, 12777, 5670, 3035, 
            339, 5163, 7129, 968, 5953, 20890, 7695, 9255, 2083, 20439, 9361, 4827, 1756, 
            12783, 4976, 11086, 6066, 675, 10749, 7767, 597, 1134, 14063, 7529, 2611, 
            10134, 4483, 3737, 35, 17345, 19921, 10216, 8516, 3091, 5444, 3201, 7079, 
            10066, 1129, 486, 17571, 10938, 10895, 19096, 26037, 2492, 4120, 3853, 10043, 
            19576, 62, 22662, 706, 5489, 4634, 24151, 7425, 7216, 2540, 5738, 1980, 
            26913, 10141, 14737, 8061, 25818, 23464, 898, 821, 3945, 25585, 2765, 8864, 
            2884, 142, 1264, 1906, 474, 722, 11512, 3772, 1356, 17240, 7226, 8389, 2376, 
            29073, 20984, 20405, 21986, 2535, 9605, 4283, 3169, 6461, 13, 8451, 2045, 
            2445, 1319, 11198, 25608, 4041, 5770, 632, 31229, 15414, 22248, 4332, 1496, 
            17043, 20379, 14506, 1876, 25561, 2251, 8007, 22979, 33014, 26000, 2912, 
            7501, 22142, 1352, 32604, 7211, 14960, 25802, 7085, 4808, 9534, 5881, 21070, 
            7893, 7018, 6365, 22549, 1688, 209, 7809, 6755, 501, 26126, 8938, 14343, 
            10268, 27328, 10089, 25911, 193, 2053, 2152, 9365, 1101, 17322, 4369, 2172, 
            4525, 22177, 2925, 7185, 25679, 18178, 21888, 1610, 29278, 20062, 23682, 
            33426, 552, 246, 27445, 4257, 33134, 9751, 9060, 15266, 1183, 26746, 22010, 
            10766, 15733, 8252, 23963, 5020, 2824, 26316, 9679, 7219, 5800, 3252, 7062, 
            6690, 33755, 22731, 75, 419, 21948, 3627, 85, 3249, 29618, 18885, 19759, 288, 
            1482, 124, 24411, 22639, 10360, 32654, 21840, 7165, 281, 5449, 23399, 671, 
            1297, 9607, 7306, 31, 5561, 2148, 4890, 2188, 4239, 89, 6152, 10113, 15214, 
            5668, 5869, 29841, 4237, 5878, 1786, 1184, 4963, 18605, 29322, 20073, 697, 
            21147, 19813, 9345, 2362, 3178, 18771, 1946, 2054, 11407, 21503, 27121, 4069, 
            1890, 3401, 13953, 19673, 3680, 7465, 189, 9289, 22760, 3292, 24060, 25061, 
            6170, 15237, 8827, 10612, 10968, 9074, 2402, 21929, 7117, 77, 452, 2637, 
            15426, 10637, 11212, 21165, 26081, 6389, 222, 18737, 1776, 24695, 2073, 668, 
            398, 4323, 17832, 6442, 15348, 3303, 3892, 14755, 25012, 1108, 23276, 33088, 
            2174, 18152, 26214, 31038, 5693, 15728, 1824, 6421, 19137, 750, 25221, 1684, 
            10670, 940, 7414, 7793, 4984, 6825, 19397, 22373, 10094, 26321, 4710, 1347, 
            109, 5984, 26446, 944, 10813, 1377, 295, 672, 21860, 24447, 1633, 33966, 
            23488, 14983, 8091, 5335, 20025, 6541, 23288, 5267, 7359, 33310, 4845, 10673, 
            28763, 7817, 66, 7572, 5861, 19638, 8884, 2416, 17272, 21296, 69, 10075, 
            8828, 1484, 1405, 4138, 2131, 2729, 225, 17871, 1799, 1288, 535, 33025, 
            11095, 5484, 3581, 18447, 7352, 4127]