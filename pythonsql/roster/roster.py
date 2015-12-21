import json
import sqlite3


def main():
    con = None
    try:
        con = sqlite3.connect('rosterdb.sqlite')
        cur = con.cursor()
        init_db(cur)

        with open('roster_data.json') as input_data:
            json_data = json.loads(input_data.read())

        db_insert = make_db_insert(cur)
        map(db_insert, json_data)
        # for entry in json_data:
        #     insert_entry(cur, entry)

        con.commit()
    finally:
        if con:
            con.close()


def init_db(cur):
    cur.executescript('''
        DROP TABLE IF EXISTS User;
        DROP TABLE IF EXISTS Member;
        DROP TABLE IF EXISTS Course;

        CREATE TABLE User (
            id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            name   TEXT UNIQUE
        );

        CREATE TABLE Course (
            id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            title  TEXT UNIQUE
        );

        CREATE TABLE Member (
            user_id     INTEGER,
            course_id   INTEGER,
            role        INTEGER,
            PRIMARY KEY (user_id, course_id)
        )
        ''')


def make_db_insert(cur):
    def db_insert(entry):
        name, title, role = entry

        cur.execute('''INSERT OR IGNORE INTO User (name)
            VALUES ( ? )''', (name, ))
        cur.execute('SELECT id FROM User WHERE name = ? ', (name, ))
        user_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO Course (title)
            VALUES ( ? )''', (title, ))
        cur.execute('SELECT id FROM Course WHERE title = ? ', (title, ))
        course_id = cur.fetchone()[0]

        cur.execute('''INSERT OR REPLACE INTO Member
            (user_id, course_id, role) VALUES ( ?, ? , ?)''', (user_id, course_id, role))
    return db_insert


if __name__ == '__main__':
    main()
