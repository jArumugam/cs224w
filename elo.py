#!/usr/bin/env python

import requests
from datetime import datetime

def get_elo_data(user_id):
    # Fetch web page from StackRating.
    response = requests.get('http://stackrating.com/user/{}'.format(user_id))
    lines = response.text.split('\n')

    # Extremely janky parser that parses the JS array as a Python list.
    parse = lambda line: tuple(eval(line.strip())[0])

    # Get line number bounds for array in sounce.
    start, end = 0, 0
    for i, line in enumerate(lines):
        if not start and line.endswith('var rows = ['):
            start = i + 1
        if start and line.endswith('];'):
            end = i 
            break

    data = sorted(parse(line) for line in lines[start:end])
    return [(datetime.fromtimestamp(i), j) for i,j in data]