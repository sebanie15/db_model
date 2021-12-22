import sqlite3


def execute(db_name):
    def wrapper(func):
        con = sqlite3.connect(db_name)

        def inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        con.commit()
        con.close()
        return inner_wrapper

    return wrapper


def replacing_operators(sql, operators):
    for operator in operators:
        sql = sql.replace("|", operator, 1)

    return sql
