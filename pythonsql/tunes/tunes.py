from os import path

import sqlite3

CUR_DIR = path.dirname(path.abspath(__file__))


def prepare_db(con):
    sql_script = path.join(CUR_DIR, "db_script.sql")
    with open(sql_script) as stmt:
        with con:
            con.executescript(stmt.read())


def main():
    con = None
    try:
        con = sqlite3.connect('tunes.sqlite')
        prepare_db(con)
    finally:
        if con:
            con.close()


if __name__ == '__main__':
    main()
