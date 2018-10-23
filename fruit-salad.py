"""Fruit Salad Data Transformation"""


import jsonlines
import re
import statistics
from pprint import pprint


class FruitSalad:
    """ Extracts and transforms data, performs analysis on data."""

    def __init__(self):

        self.data = []
        self.transformed = []

    def get_data(self):
        """ Extracts data from JSON Lines file and returns a list of objects, 
            where each object is a transformed version of the corresponding 
            object in the original file.
        """

        # url = "https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/simple-etl.jsonl"
        url = "data/simple-etl.jsonl"
        fields = ['name', 'age', 'isActive', 'favoriteFruit', 'balance', 'posts']

        # If data is not already downloaded, then run below block to download
        # and parse data; if data is already downloaded, then simply return it
        # below.
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
                print("Error: No data found!")

        return self.data


    def transform_data(self):
        """ Makes a sub-dictionary from original data and adds it to a list.

            Test:

            >>> fruit = FruitSalad()
            >>> bool(fruit.transform_data())
            True
        """

        data = self.get_data()

        # Caching the data: if data has already been transformed, just return
        # it; otherwise, run through the below block to transform the data.
        if not self.transformed:

            for d in data:

                post_lst = []

                # Loop through list of dictionaries (posts) to find words 
                # regardless of punctuation and convert them to lowercase.
                for text in d['posts']:
                    post = re.findall(r'\w+', text['post'].lower())
                    post_lst.append(post)
                    
                # post_lst is a list of lists of all words from the text in 
                # every post. Then we flatten the list.
                flatten = [leaf for tree in post_lst for leaf in tree]

                # Update dictionary with multiple entries at a time
                sub = {}
                sub.update({ \
                    'full_name': str(d['name']['first'] + " " + d['name']['last']), \
                    'post_count': len(d['posts']), \
                    'most_common_word_in_posts': self.find_most_common_word(flatten), \
                    'age': d['age'], \
                    'is_active': d['isActive'], \
                    'favorite_fruit': str(d['favoriteFruit']), \
                    'balance': float(d['balance'].strip('$').replace(',', ''))
                })

                self.transformed.append(sub)

        return self.transformed


    def find_most_common_word(self, lst):
        """ Takes in list of words and returns the most common word(s) in a list.
            
            Test:
            >>> fruit = FruitSalad()
            >>> fruit.find_most_common_word( \
                    [ 'me', 'me', 'me', 'i', 'will', 'succeed', 'succeed'])
            ['me']
        """
        
        word_lst = []
        freq = {}
        highest_freq = 0

        # Create dictionary freq with word: count
        for word in lst:
            if word not in freq:
                freq[word] = 1
            else:
                freq[word] += 1

        # Iterate through the dict and find the highest frequency.
        # Find the word where the value matches the highest frequency and
        # append to word_lst.
        for word, count in freq.items():
            if count > highest_freq:
                word_lst = [] # Make sure list is completely empty.
                word_lst.append(str(word))
                highest_freq = count

            # If count is the same, add the word to existing word_lst
            elif count == highest_freq:
                word_lst.append(str(word))

        return word_lst


    def get_total_posts(self):
        """ Returns total number of posts in the dataset.
            
            Test:

            >>> fruit = FruitSalad()
            >>> type(fruit.get_total_posts())
            <class 'int'>
        """

        data = self.transform_data()
        total_posts = 0

        for user in data:
            total_posts += user['post_count']

        return total_posts


    def get_mc_overall_word(self):
        """ Returns most common of the most common words in the dataset."""

        data = self.transform_data()
        words = []

        for user in data:
            words.append(user['most_common_word_in_posts'])

        flatten = [leaf for tree in words for leaf in tree]

        return self.find_most_common_word(flatten)


    def get_total_bal(self):

        data = self.transform_data()
        total = 0

        for user in data:
            total += user['balance']

        # Convert float to string of 2 decimal places, then convert back to a
        # float.
        return float(format(total, '.2f'))


    def get_mean_bal(self):

        data = self.transform_data()

        bal_lst = []

        for user in data:
            bal_lst.append(user['balance'])

        return float(format(statistics.mean(bal_lst), '.2f'))


    def get_active_mean(self):

        data = self.transform_data()

        active_bal = []

        for user in data:
            if user['is_active']:
                active_bal.append(user['balance'])

        return float(format(statistics.mean(active_bal), '.2f'))


    def get_strawberry_mean(self):

        data = self.transform_data()

        strawberry = []

        for user in data:
            if user['favorite_fruit'] == 'strawberry':
                strawberry.append(user['balance'])

        return float(format(statistics.mean(strawberry), '.2f'))


# transform data:
#  {'age': 20,
#   'balance': 3317.36,
#   'favorite_fruit': 'strawberry',
#   'full_name': 'Fox Cummings',
#   'is_active': True,
#   'most_common_word_in_posts': ['in', 'minim'],
#   'post_count': 10},
#  {'age': 38,
#   'balance': 1939.57,
#   'favorite_fruit': 'banana',
#   'full_name': 'Marilyn Sweeney',
#   'is_active': False,
#   'most_common_word_in_posts': ['elit'],
#   'post_count': 10},


if __name__ == "__main__":
    
    import doctest

    fruit = FruitSalad()
    pprint(fruit.transform_data())
    print(fruit.get_total_posts())
    print(fruit.get_mc_overall_word())
    print(fruit.get_total_bal())
    print(fruit.get_mean_bal())
    print(fruit.get_active_mean())
    print(fruit.get_strawberry_mean())


    result = doctest.testmod()
    if result.failed == 0:
        print("\nALL TESTS PASSED\n")

