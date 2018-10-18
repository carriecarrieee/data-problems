"""Fruit Salad Data Transformation"""


import jsonlines
from pprint import pprint


class FruitSalad:
    """ Extracts and transforms data, performs analysis on data."""

    def __init__(self):

        self.data = []

    def get_data(self):
        """ Extracts data from JSON Lines file and returns a list of objects, 
            where each object is a transformed version of the corresponding 
            object in the original file.
        """

        # url = "https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/simple-etl.jsonl"
        url = "data/simple-etl.jsonl"
        fields = ['name', 'age', 'isActive', 'favoriteFruit','balance']

        if not self.data:

            try:
                with jsonlines.open(url, mode="r") as reader:
                    d = reader.iter(type=dict, allow_none=True)

                    # Loop through dict to get only selected fields for a new 
                    # sub dict
                    for obj in d:
                        self.data.append({key: obj[key] for key in fields \
                            if key in obj})

            except:
                print "Error: No data found!"

        return self.data


    def transform_data(self):
        """ Makes a sub-dictionary from original data and adds it to a list."""

        data = self.get_data()
        result = []

        for d in data:
            sub = {}

            sub.update({ \
                'full_name': str(d['name']['first'] + " " + d['name']['last']), \
                'age': d['age'], \
                'is_active': d['isActive'], \
                'favorite_fruit': str(d['favoriteFruit']), \
                'balance': float(d['balance'].strip('$').replace(',', ''))
            })
            result.append(sub)

        return result





fruit = FruitSalad()
pprint(fruit.transform_data())

# current data set:
 # {'age': 20,
 #  'balance': u'$3,317.36',
 #  'favoriteFruit': u'strawberry',
 #  'isActive': True,
 #  'name': {u'first': u'Fox', u'last': u'Cummings'}},
 # {'age': 38,
 #  'balance': u'$1,939.57',
 #  'favoriteFruit': u'banana',
 #  'isActive': False,
 #  'name': {u'first': u'Marilyn', u'last': u'Sweeney'}},

