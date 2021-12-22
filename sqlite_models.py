import os

import sqlite3
from sqlite3.dbapi2 import Connection

from db_models import DatabaseModel, SessionModel, execute


def replacing_operators(sql, operators):
    for operator in operators:
        sql = sql.replace("|", operator, 1)

    return sql


class Database(DatabaseModel):

    r"""
    This is class to manage connections by SQL statements

    Basic usage:

      >> from sqlite_models import Database
      >> my_database = Database(path='./path_to_file')

    .versionadded:: 1.0.0

    """
    ALTER_TABLE_FUNCTIONS = {'ADD', 'ALTER COLUMN', 'MODIFY COLUMN', 'MODIFY', 'DROP COLUMN'}

    def __init__(self, data_source: str, db_name: str, guild_file: str = './create_guild.sql') -> None:
        super().__init__(db_name)
        if not os.path.isfile(data_source):
            try:
                with open(guild_file) as guild:
                    self.script(guild.read())
            except FileNotFoundError:
                print(f'File {guild_file} not found!')

        self._db_name = data_source
        self.__connection = None

    def script(self, value):
        self.connection.cursor().executescript(value)

    @property
    def db_name(self):
        return self._db_name

    @property
    def connection(self) -> Connection:
        """ This method return connection <Connection> with the database 
        or ValueError in case of no database binding 
        """
        if self.__connection is None:
            raise ValueError(f"")
        return self.__connection

    def connect(self, engine: str):
        """ This method connecting t database

        :param engine: type of database engine
        """
        if engine == 'sqlite':
            self.__connection = sqlite3.connect(self._db_name)

    @execute(db_name=db_name)
    def create_table(self, table: str, **kwargs) -> str:
        """ Method to generate sql statement CREATE TABLE

        :param table: str -> name of table
        :param kwargs: ** -> columns and types of data.

        .. versionadded:: 1.0.0

        """
        query = f'CREATE TABLE {table} ('
        query += ', '.join(f'{key} {value}' for key, value in kwargs.items())
        query += ');'

        return query

    @execute(db_name=db_name)
    def alter_table(self, table: str, func: str = 'ADD', *args, **kwargs) -> str:
        """ This method generate ALTER TABLE statements.
        The ALTER TABLE statements is used to add, delete, or modify columns ina an existing table

        :param table: table name
        :param func: function in ALTER STATEMENTS
        :param args: names of columns - only for DROP COLUMN
        :param kwargs: names and types of columns to add or modify
        """
        query = None
        if func == 'DROP COLUMN':
            query = f"ALTER TABLE {table} {func}" \
                    + ', '.join(",".join([value for value in args])) + ';'
        elif func in self.ALTER_TABLE_FUNCTIONS:
            query = f"ALTER TABLE {table} {func} " \
                    + ', '.join(f'{key} {value}' for key, value in kwargs.items()) + ';'
        else:
            raise ValueError('Wrong type of function')

        return query

    def drop_table(self, table: str):
        """ This method generate DROP TABLE statements.

        :param table: table name
        """
        pass

    def truncate_table(self, table: str):
        """ This method generate TRUNCATE TABLE statements.

        :param table: table name
        """
        pass

    def create_index(self, index_name: str, **kwargs):
        """ This method generate CREATE INDEX statements.

         :param index_name: name of the index
         :param kwargs: names of table and tuple of columns
         """
        pass

    def create_uindex(self, index_name: str, **kwargs):
        """ This method generate CREATE UNIQUE INDEX statements.

        :param index_name: name of the unique index
        :param kwargs: names of table and tuple of columns
        """
        pass

    def drop_index(self, index_name: str, *args):
        """ This method generate CREATE UNIQUE INDEX statements.

        :param index_name: name of the unique index
        :param args: for MS Access, SQL Server and MySQL - name of table
        """
        pass


class Session(SessionModel):

    @execute
    def insert(self, table: str, *args, **kwargs) -> str:
        """
        function to generate sql statement INSERT INTO

        :param table: name of table
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """

        query = f'INSERT INTO {table} '
        if args:
            query += f'VALUES ({",".join([value for value in args])});'
        elif kwargs:
            query += f'({",".join([key for key in kwargs.keys()])}) '
            query += f'VALUES ({",".join([value for value in kwargs.values()])});'
        return query

    @execute
    def insert_or_replace(self, table: str, *args, **kwargs) -> str:
        """
        function to generate sql statement INSERT INTO OR REPLACE

        :param table: name of table
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """

        query = f'INSERT OR REPLACE INTO {table} '
        if args:
            query += f'VALUES ({",".join([value for value in kwargs.values()])});'
        elif kwargs:
            query += f'({",".join([key for key in kwargs.keys()])}) '
            query += f'VALUES ({",".join([value for value in kwargs.values()])});'
        return query

    def insert_many(self, table: str, *args, **kwargs) -> None:
        """
        function to generate sql statement INSERT INTO

        :param table: table name
        :param args: values
        :param kwargs: columns and values, ie col1='val1'
        """
        query = f'INSERT INTO {table} '  # TODO

        self.__cursor.executemany(query, args)

    def fetch_all(self, table: str, *operators,  **conditions):
        """
        Method to fetch all data

        :param table: name of table
        :param operators: tuple of operators eg. ('=',)
        :param conditions: dict of conditions eg. col1='value1'
        """
        values = conditions.values()
        sql = f"SELECT * FROM {table} WHERE {' and '.join([f'{condition}|?' for condition in conditions])}"
        sql = replacing_operators(sql, operators)

        query = self.__cursor.execute(sql, list(values))
        return query.fetchall()

    def insert_or_ignore(self, table, values):
        sql = f"INSERT OR IGNORE INTO {table} VALUES ({','.join(['?' for _ in values])})"
        self.__cursor.execute(sql, values)

        # func(table, ((?, ? ...), (?, ? ...), ...))

    def script_insert_or_ignore(self, table, inserts):
        values = f"({','.join(['?' for _ in inserts[0]])})"
        self.__cursor.executemany(f"INSERT OR IGNORE INTO {table} VALUES {values};", inserts)

        # func(table, ((?, ? ...), (?, ? ...), ...))

    def script_insert(self, table, inserts):
        values = f"({','.join(['?' for _ in inserts[0]])})"
        self.__cursor.executemany(f"INSERT INTO {table} VALUES {values};", inserts)

        # func(table, ((?, ? ...), (?, ? ...), ...))

    def script_insert_or_replace(self, table, inserts):
        values = f"({','.join(['?' for _ in inserts[0]])})"
        self.__cursor.executemany(f"INSERT OR REPLACE INTO {table} VALUES {values};", inserts)

        # func(table, column1 = ?, column2 = ?, ...)

    def update_record(self, table, **conditions):
        values = conditions.values()
        sql = f"UPDATE {table} SET {' WHERE '.join([f'{condition}=?' for condition in conditions])}"
        query = self.__cursor.execute(sql, list(values))

        # func(table, column, ("op", "op2"), column = ?, column2 = ?)

    def update_value(self, table, column, operators, **conditions):
        values = conditions.values()
        sql = f"UPDATE {table} SET {column} = {' WHERE '.join([f'{condition}|?' for condition in conditions])}"
        sql = replacing_operators(sql, operators)
        query = self.__cursor.execute(sql, list(values))

    def script_update_value(self, table, column, values, **conditions):
        operators = conditions.values()

        sql = f"UPDATE {table} SET {column} = {' WHERE '.join([f'{condition}|?' for condition in conditions])};"
        sql = replacing_operators(sql, operators)
        sql = sql.replace(f"{column}=?", "?")

        self.__cursor.executemany(sql, values)

    def fetch_by_columns(self, table, columns, operators, **conditions):
        values = conditions.values()
        sql = f"SELECT {','.join([column for column in columns])} FROM {table} WHERE {' and '.join([f'{condition}|?' for condition in conditions])}"
        sql = replacing_operators(sql, operators)

        query = self.__cursor.execute(sql, list(values))
        return query.fetchall()

    def fetch_all_in_order(self, table, order_by, operators, **conditions):
        values = conditions.values()
        sql = f"SELECT * FROM {table} ORDER BY {' , '.join([f'{condition}' for condition in conditions])} {order_by}"
        sql = replacing_operators(sql, operators)

        query = self.__cursor.execute(sql, list(values))
        return query.fetchall()

        # func(table, ("op1", op2, ...), cond1 = "?", cond2 = "?", ...)

    def delete(self, table, operators, **conditions):
        values = conditions.values()
        sql = f"DELETE FROM {table} WHERE {' and '.join([f'{condition}|?' for condition in conditions])}"
        sql = replacing_operators(sql, operators)

        self.__cursor.execute(sql, list(values))
        self.commit()

    def fetch_distinct(self, table, column):
        sql = f'SELECT DISTINCT {column} FROM {table}'
        query = self.__cursor.execute(sql)
        return query.fetchall()

    def execute_script(self, sql, values):
        self.__cursor.execute(sql, values)