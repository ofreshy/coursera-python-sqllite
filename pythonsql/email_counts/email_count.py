
from collections import Counter
from os import path

import re
import sqlite3

CUR_DIR = path.dirname(path.abspath(__file__))
EMAIL_REGEX = re.compile('^From .+?@(\S*[a-zA-Z]) ?')


def main():
    input_file = path.join(CUR_DIR, "mbox.txt")
    with open(input_file) as fs:
        email_counts = Counter()
        for line in fs:
            m = EMAIL_REGEX.match(line)
            if m:
                email_counts[m.group(1)] += 1

        print(email_counts.most_common(10))
        con = None
        try:
            con = sqlite3.connect('emaildb.sqlite')
            clean_db(con)
            insert_into_db(con, email_counts)
        finally:
            if con:
                con.close()
        print("inserted %s email counts" % len(email_counts))


def insert_into_db(con, email_counts):
    try:
        with con:
            stmt = "INSERT INTO Counts (org, count) VALUES ( ?, ? )"
            con.executemany(stmt, email_counts.items())
    except sqlite3.IntegrityError:
        print('failed to insert!')
        raise


def clean_db(con):
    try:
        with con:
            con.execute('DROP TABLE IF EXISTS Counts')
            con.execute('CREATE TABLE Counts (org TEXT, count INTEGER)')
    except sqlite3.IntegrityError:
        print('failed to drop/create DB!')
        raise


if __name__ == '__main__':
    main()
