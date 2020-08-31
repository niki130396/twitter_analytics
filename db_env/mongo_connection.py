from pymongo import MongoClient
from pymongo.cursor import Cursor
from abc import ABC, abstractmethod
import pandas as pd


class MongoConnection(ABC):
    @abstractmethod
    def __init__(self):
        self.client = MongoClient()
        self.database = self.client.twitter
        self.collection = self.database.tweet


class QueryDispatcher(MongoConnection):
    def __init__(self):
        super().__init__()
        self.query_result = None
        self.__select_params = {}
        self.__where_params = {}

    def queryDatabase(self, columns=None, where_clause=None):
        """
        SELECT id,
            user_id,
            status
        FROM people

            TRANSLATES TO

        db.people.find(
                        { },
                        { user_id: 1, status: 1 }
                        );

        SELECT user_id, status
        FROM people
        WHERE status = "A"

        TRANSLATES TO

        db.people.find(
                        { status: "A" },
                        { user_id: 1, status: 1, _id: 0 }
                        )

        :param columns: dict
        :param where_clause: dict
        :return: dict
        """
        if columns is None and where_clause is None:
            return self.collection.find({})
        elif columns is not None and where_clause is not None:
            return self.collection.find(where_clause, columns)
        elif columns is not None:
            return self.collection.find({},
                                        columns)
        else:
            return self.collection.find(where_clause)

    def select(self, columns={}):
        if columns:
            self.query_result = self.collection.find({}, columns)
        else:
            self.query_result = self.collection.find({})
        self.__select_params = columns
        return self

    def where(self, clause={}):
        #TODO FIGURE OUT A WAY NOT TO MAKE A NEW CALL TO THE DATABASE FOR THE WHERE CLAUSE
        self.query_result = self.collection.find(clause, self.__select_params)
        self.__where_params = clause
        return self


class DataExplorer(MongoConnection):
    def __init__(self):
        super().__init__()

    def toDataFrame(self, query_result: Cursor) -> pd.DataFrame:
        return pd.DataFrame(query_result)


if __name__ == '__main__':
    obj = QueryDispatcher()
    query = obj.select({"created_at": 1, "text": 1, "retweet_count": 1}).where({"retweet_count": 0}).query_result
    data_explorer = DataExplorer()
    print(data_explorer.toDataFrame(query))