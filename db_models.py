import sqlite3
from abc import ABC, abstractmethod



def execute(db_name):
    def wrapper(func):
        con = sqlite3.connect(db_name)

        def inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        con.commit()
        con.close()
        return inner_wrapper

    return wrapper


class TableModel(ABC):
    """ Abstract Table class """
    pass


class DatabaseModel(ABC):
    """ Abstract Database class """

    def __init__(self, db_name: str):
        self.__db_name = db_name
        self.__connection = None

    def commit(self):
        self.__connection.commit()

    def close(self):
        self.__connection.close()

    @abstractmethod
    def connect(self, engine: str):
        pass

    @abstractmethod
    def create_table(self, table: str, **kwargs) -> str:
        """       Method to generate sql statement CREATE TABLE

        :param table: str -> name of table
        :param kwargs: ** -> columns and types of data.

        .. versionadded:: 1.0.0

        """
        pass

    @abstractmethod
    def alter_table(self, table: str, func: str = 'ADD', *args, **kwargs) -> str:
        """ This method generate ALTER TABLE statements.
        The ALTER TABLE statements is used to add, delete, or modify columns ina an existing table

        :param table: table name
        :param func: function in ALTER STATEMENTS
        :param args: names of columns - only for DROP COLUMN
        :param kwargs: names and types of columns to add or modify
        """
        pass

    @abstractmethod
    def drop_table(self, table: str):
        """ This method generate DROP TABLE statements.

        :param table: table name
        """
        pass

    @abstractmethod
    def truncate_table(self, table: str):
        """ This method generate TRUNCATE TABLE statements.

        :param table: table name
        """
        pass

    @abstractmethod
    def create_index(self, index_name: str, **kwargs):
        """ This method generate CREATE INDEX statements.

        :param index_name: name of the index
        :param kwargs: names of table and tuple of columns
        """
        pass

    @abstractmethod
    def create_uindex(self, index_name: str, **kwargs):
        """ This method generate CREATE UNIQUE INDEX statements.

        :param index_name: name of the unique index
        :param kwargs: names of table and tuple of columns
        """
        pass

    @abstractmethod
    def drop_index(self, index_name: str, *args):
        """ This method generate CREATE UNIQUE INDEX statements.

        :param index_name: name of the unique index
        :param args: for MS Access, SQL Server and MySQL - name of table
        """
        pass


class SessionModel(ABC):
    """ Abstract Session class"""

    def __init__(self, db: DatabaseModel):
        self.__db = None
        self.__cursor = None

    def bind(self, db: DatabaseModel):
        if isinstance(db, DatabaseModel):
            self.__db = db
        else:
            raise TypeError(f"It's not a Database class")

    def __enter__(self):
        """ method on enter - context manager """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ methid on exit - context manager"""
        self.commit()
        self.close()

    def connect(self) -> None:
        """ make a connection to the database """
        if isinstance(self.__db, DatabaseModel):
            self.__db.connect()
            self.__cursor = self.__db.connection.cursor()
        else:
            raise TypeError(f"{self.__db} is not a Database class")

    def commit(self):
        """ commit the changes
        """
        self.__db.commit()

    def close(self):
        """ close the connection """
        self.__db.close()

    @abstractmethod
    def insert(self, table: str, *args, **kwargs) -> str:
        """
        function to generate sql statement INSERT INTO

        :param table: name of table
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """
        pass

    @abstractmethod
    def insert_or_replace(self, table: str, *args, **kwargs) -> str:
        """
        function to generate sql statement INSERT INTO OR REPLACE

        :param table: name of table
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """
        pass

    @abstractmethod
    def insert_many(self, table: str, *args, **kwargs) -> None:
        """
        function to generate sql statement INSERT INTO

        :param table: table name
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """
        pass

    @abstractmethod
    def fetch_all(self, table: str, *operators, **conditions):
        """
        Method to fetch all data

        :param table: name of table
        :param operators: tuple of operators eg. ('=',)
        :param conditions: dict of conditions eg. col1='value1'
        """
        pass
