#!/usr/bin/env python

import requests

def get_elo_data(user_id):
    # Fetch web page from StackRating.
    response = requests.get('http://stackrating.com/user/{}'.format(user_id))
    
    # List to collect output.
    data = []

    # Extremely janky parser that parses the JS array as a Python list.
    parse = lambda line: tuple(eval(line.strip())[0])

    # Parse each line the ranking array.
    in_array = False
    for i, line in enumerate(response.text.split('\n')):
        if line.endswith('var rows = ['):
            in_array = True
            continue
        if in_array:
            if line.endswith('];'):
                break
            data.append(parse(line))

    return data