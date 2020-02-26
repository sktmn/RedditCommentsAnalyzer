from CommentsSchema import Schema


# schema = CommentsSchema.Schema.get()

def final_clause(sub_list: list) -> str:
    return ' where subreddit in (' + ', '.join(map(lambda s: '\'' + s + '\'', sub_list)) + ') '


class QueryGenerator:

    @staticmethod
    def columns() -> str:
        col_list = [Schema.Author, Schema.Subreddit, Schema.Ups, Schema.Body]
        return ', '.join(col_list)

    @staticmethod
    def cars() -> str:
        sub_list = ['cars', 'WhatCarShouldIBuy', 'UsedCars', 'AskCarSales', 'CarReviews']
        return final_clause(sub_list)

    @staticmethod
    def phones() -> str:
        sub_list = ['phones', 'Smartphones', 'suggestasmartphone', 'PickMeAPhone', 'apple', 'iphone',
                    'PickAnAndroidForMe', 'androidphones']
        return final_clause(sub_list)


# print(QueryGenerator.cars())
# print(QueryGenerator.mobile_phones())
# print(QueryGenerator.columns())
