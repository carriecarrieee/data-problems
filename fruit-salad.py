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


fruit = FruitSalad()
pprint(fruit.get_data())

