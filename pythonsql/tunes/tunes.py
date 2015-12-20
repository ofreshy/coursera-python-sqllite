from os import path
from xml.etree import ElementTree

import sqlite3

CUR_DIR = path.dirname(path.abspath(__file__))


def main():
    xml_file_name = path.join(CUR_DIR, "Library.xml")
    with open(xml_file_name) as xml_feed:
        xml_nodes = ElementTree.parse(xml_feed).findall('dict/dict/dict')

    records = generate_records(xml_nodes)

    con = None
    try:
        con = sqlite3.connect('tunes.sqlite')
        with con:
            sql_script = path.join(CUR_DIR, 'db_script.sql')
            with open(sql_script) as stmt:
                init_statement = stmt.read()
            con.executescript(init_statement)

        with con:
            cur = con.cursor()
            insert_records(cur, records)

    except sqlite3.IntegrityError:
        print('Houston , we cannot open connection')
    finally:
        if con:
            con.close()


def generate_records(nodes):
    all_tags = ('Artist', 'Genre', 'Album', 'Track ID', 'Name', 'Total Time', 'Rating', 'Play Count')
    required_tags = ('Track ID', 'Name', 'Album', 'Artist', 'Genre')
    for node in nodes:
        record = {}
        it = iter(node)
        key = next(it, None)
        value = next(it, None)
        while key is not None and value is not None:
            if key.text in all_tags:
                record[key.text] = value.text
            key = next(it, None)
            value = next(it, None)

        if all([x in record for x in required_tags]):
            yield record


def insert_records(cur, records):
    def insert_author(artist_name):
        cur.execute('INSERT OR IGNORE INTO Artist (name) VALUES (?)', (artist_name, ))
        cur.execute('SELECT id FROM Artist WHERE name = ? ', (artist_name, ))
        return cur.fetchone()[0]

    def insert_genre(genre_name):
        cur.execute('INSERT OR IGNORE INTO Genre (name) VALUES (?)', (genre_name, ))
        cur.execute('SELECT id from Genre WHERE name=?', (genre_name, ))
        return cur.fetchone()[0]

    def insert_album(title, artist_id):
        cur.execute('INSERT OR IGNORE INTO Album (title, artist_id) VALUES (?, ?)', (title, artist_id))
        cur.execute('SELECT id from Album WHERE title=?', (title, ))
        return cur.fetchone()[0]

    def insert_track(title, album_id, genre_id, length, rating, play_count):
        cur.execute('SELECT id from Track WHERE title=? AND album_id=?', (title, album_id))
        if not cur.fetchone():
            cur.execute('INSERT INTO Track '
                        '(title, album_id, genre_id, len, rating, count) '
                        'VALUES (?, ?, ?, ?, ?, ?)',
                        (title, album_id, genre_id, length, rating, play_count))
        cur.execute('SELECT id from Track WHERE title=? AND album_id=?', (title, album_id))
        return cur.fetchone()[0]

    def insert_record(record):
        artist_id = insert_author(record['Artist'])
        genre_id = insert_genre(record['Genre'])
        album_id = insert_album(record['Album'], artist_id)

        track_title = record['Name']
        track_length = record.get('Total Time', 0)
        rating = record.get('Rating', 0)
        play_count = record.get('Play Count', 0)
        insert_track(track_title, album_id, genre_id, track_length, rating, play_count)

    for i, r in enumerate(records):
        insert_record(r)

    print('Inserted %s records' % i)

    def get_count(table_name):
        cur.execute("SELECT COUNT(*) FROM %s;" % table_name)
        return cur.fetchone()[0]

    print('Albums : %s' % get_count('Album'))
    print('Genres : %s' % get_count('Genre'))
    print('Artists: %s' % get_count('Artist'))
    print('Tracks: %s '% get_count('Track'))





if __name__ == '__main__':
    main()
