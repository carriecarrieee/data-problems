"""Fruit Salad Data Transformation"""


import requests
import jsonlines
import re
import statistics
from collections import Counter
from pprint import pprint


class FruitSalad:
    """ Extracts and transforms data, performs analysis on data."""

    def __init__(self):

        self.data = []
        self.transformed = []


    def get_data(self):
        """ Extracts data from JSON Lines file and returns a list of objects.
        """

        url = "https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/simple-etl.jsonl"
        # url = "data/simple-etl.jsonl"
        fields = ['name', 'age', 'isActive', 'favoriteFruit', 'balance', 'posts']

        # If data is not already downloaded, then run below block to download
        # and parse data; if data is already downloaded, then simply return it
        # below.
        if not self.data:

            try:
                r = requests.get(url)

                assert(r.status_code == 200), "Response to data request \
                    failed."

                # Response object returns a giant string; split it on \n because
                # jsonlines is a list of json objects delimited by \n
                json_obj = r.text.split('\n')

                d = jsonlines.Reader(json_obj).iter(type=dict, 
                                                    allow_none=True, 
                                                    skip_invalid=True)

                # Loop through dict to get only selected fields for a new 
                # sub dict
                for obj in d:
                    self.data.append({key: obj[key] for key in fields \
                        if key in obj})

            except:
                print("Error: No data found!")

        return self.data


    def transform_data(self):
        """ Makes a sub-dictionary from original data, where each object will be
            a transformed version of the corresponding object in the original 
            file, and adds it to a list.

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
                sub = {
                    'full_name': str(d['name']['first'] + " " + d['name']['last']),
                    'post_count': len(d['posts']),
                    'most_common_word_in_posts': self.get_most_common_word(flatten),
                    'age': d['age'],
                    'is_active': d['isActive'],
                    'favorite_fruit': str(d['favoriteFruit']),
                    'balance': float(d['balance'].strip('$').replace(',', ''))
                }

                self.transformed.append(sub)

        return self.transformed


    def get_most_common_word(self, lst):
        """ Takes in list of words and returns the most common word(s) in a list.
            
            Test:
            >>> fruit = FruitSalad()
            >>> fruit.get_most_common_word( \
                    [ 'me', 'me', 'me', 'i', 'will', 'succeed', 'succeed'])
            ['me']
        """
        
        # Create dictionary-like Counter of {word: frequency}
        counter = Counter(lst)

        # Find highest frequency out of all the frequencies
        max_count = max(counter.values())

        # Store the most common word in a list where the value (freq) matches
        # the max_count.
        mode = [k for k, v in counter.items() if v == max_count]

        return mode


    def get_total_posts(self, data):
        """ Returns total number of posts in the dataset.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_total_posts(fruit.transform_data())
            500
        """

        total_posts = 0

        for user in data:
            total_posts += user['post_count']

        return total_posts


    def get_mc_overall_word(self, data):
        """ Returns most common of the most common words in the dataset.

            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_mc_overall_word(fruit.transform_data())
            ['incididunt']

        """

        words = []

        for user in data:
            words.append(user['most_common_word_in_posts'])

        flatten = [leaf for tree in words for leaf in tree]

        return self.get_most_common_word(flatten)


    def get_total_bal(self, data):
        """ Returns sum of all account balances for all users.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_total_bal(fruit.transform_data())
            124797.76
        """

        total = 0

        for user in data:
            total += user['balance']

        # Convert float to string of 2 decimal places, then convert back to a
        # float.
        return float(format(total, '.2f'))


    def get_mean_bal(self, data):
        """ Returns avg account balance for all users.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_mean_bal(fruit.transform_data())
            2495.96
        """

        bal_lst = []

        for user in data:
            bal_lst.append(user['balance'])

        return float(format(statistics.mean(bal_lst), '.2f'))


    def get_active_mean_bal(self, data):
        """ Returns avg account balance for all active users.

            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_active_mean_bal(fruit.transform_data())
            2544.72
        """

        active_bal = []

        for user in data:
            if user['is_active']:
                active_bal.append(user['balance'])

        return float(format(statistics.mean(active_bal), '.2f'))


    def get_strawberry_mean(self, data):
        """ Returns avg account balance for users who favor strawberries.

            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_strawberry_mean(fruit.transform_data())
            2214.9
        """

        strawberry = []

        for user in data:
            if user['favorite_fruit'] == 'strawberry':
                strawberry.append(user['balance'])

        return float(format(statistics.mean(strawberry), '.2f'))


    def get_min_max_mean_median(self, data):
        """ Returns the min, max, mean, and median age of all users in a list.
        
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_min_max_mean_median(fruit.transform_data())[0]
            20

            >>> fruit.get_min_max_mean_median(fruit.transform_data())[1]
            40

            >>> fruit.get_min_max_mean_median(fruit.transform_data())[2]
            30.2

            >>> fruit.get_min_max_mean_median(fruit.transform_data())[3]
            31.0

        """

        age = []

        for user in data:
            age.append(user['age'])

        return [min(age), max(age), statistics.mean(age), statistics.median(age)]


    def get_apple_lovers_age(self, data):
        """ Returns age with the most users who favor apples.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_apple_lovers_age(fruit.transform_data())
            32
        """

        apple = []

        for user in data:
            if user['favorite_fruit'] == 'apple':
                apple.append(user['age'])

        return statistics.mode(apple)


    def get_non_apple_age(self, data):
        """ Returns min and max age of the set of users who do NOT favor apples.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_non_apple_age(fruit.transform_data())[0]
            20

            >>> fruit.get_non_apple_age(fruit.transform_data())[1]
            38
        """

        non_apple = []

        for user in data:
            if user['favorite_fruit'] != 'apple':
                non_apple.append(user['age'])

        return [min(non_apple), max(non_apple)]


    def get_mc_fruit_active(self, data):
        """ Returns list of most common favorite fruit(s) for active users.
            
            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_mc_fruit_active(fruit.transform_data())
            ['banana']

        """

        fruit = []

        for user in data:
            if user['is_active']:
                fruit.append(user['favorite_fruit'])

        return self.get_most_common_word(fruit)


    def get_mc_fruit_median_age(self, data):
        """ Returns most common fruit(s) for users of the median age.

            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_mc_fruit_median_age(fruit.transform_data())
            ['strawberry', 'banana']

        """

        fruit_median_age = []

        for user in data:
            if user['age'] == self.get_min_max_mean_median(data)[3]:
                fruit_median_age.append(user['favorite_fruit'])

        return self.get_most_common_word(fruit_median_age)


    def get_acct_bal_gt_mean(self, data):
        """ Returns most common favorite fruit(s) for users with a balance
            greater than the mean.

            Test:

            >>> fruit = FruitSalad()
            >>> fruit.get_acct_bal_gt_mean(fruit.transform_data())
            ['banana']

        """

        fruit_high_bal = []

        for user in data:
            if user['balance'] > self.get_mean_bal(data):
                fruit_high_bal.append(user['favorite_fruit'])

        return self.get_most_common_word(fruit_high_bal)


    def create_summary(self):
        """ Generates an overall summary (dict) output of stats from the dataset.

            Test:

            >>> fruit = FruitSalad()
            >>> type(fruit.create_summary())
            <class 'dict'>

            >>> len(fruit.create_summary())
            5
        """

        data = self.transform_data()

        summary = { 
            'total post count': self.get_total_posts(data),
            'most_common_word_overall': self.get_mc_overall_word(data),
            'account_balance': {
                'total': self.get_total_bal(data),
                'mean': self.get_mean_bal(data),
                'active_user_mean': self.get_active_mean_bal(data),
                'strawberry_lovers_mean': self.get_strawberry_mean(data),
                },
            'age': {
                'min': self.get_min_max_mean_median(data)[0],
                'max': self.get_min_max_mean_median(data)[1],
                'mean': self.get_min_max_mean_median(data)[2],
                'median': self.get_min_max_mean_median(data)[3],
                'age_with_most_apple_lovers': self.get_apple_lovers_age(data),
                'youngest_age_hating_apples': self.get_non_apple_age(data)[0],
                'oldest_age_hating_apples': self.get_non_apple_age(data)[1],
                },
            'favorite_fruit': {
                'active_users': self.get_mc_fruit_active(data),
                'median_age': self.get_mc_fruit_median_age(data),
                'acct_balance_gt_mean': self.get_acct_bal_gt_mean(data)
                }
            }

        return summary


    def username_starts_with(self, data, char):
        """ Returns items where the user's name begins with the given character
            as a pass parameter.
        """

        users_with_char = []

        for user in data:
            if user['full_name'][0] == char.upper():
                users_with_char.append(user) 

        return users_with_char


if __name__ == "__main__":
    
    import doctest

    fruit = FruitSalad()
    # pprint(fruit.get_data())
    data = fruit.transform_data()
    pprint(fruit.username_starts_with(data, 'j'))
    # print(fruit.get_total_posts(data))
    # print(fruit.get_mc_overall_word())
    # print(fruit.get_total_bal(data))
    # print(fruit.get_mean_bal(data))
    # print(fruit.get_active_mean_bal(data))
    # print(fruit.get_strawberry_mean(data))
    # print(fruit.get_min_max_mean_median(data))
    # print(fruit.get_apple_lovers_age(data))
    # print(fruit.get_non_apple_age(data))
    # print(fruit.get_mc_fruit_active(data))
    # print(fruit.get_mc_fruit_median_age(data))
    # print(fruit.get_acct_bal_gt_mean(data))
    pprint(fruit.create_summary())

    result = doctest.testmod()
    if result.failed == 0:
        print("\nALL TESTS PASSED\n")

