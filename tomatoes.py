"""
Simple utilities for working with the Rotten Tomatoes API.
"""
import json
import re
import sqlite3
import urllib, urllib2

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

RTAPI_KEY = 'Put yer Rotten Tomatoes API key here'
RTAPI_URL = 'http://api.rottentomatoes.com/api/public/v1.0/movies.json'
OMDBAPI_URL = 'http://www.omdbapi.com/?i=%s&r=json'
PAREN = re.compile('\((.*?)\)')
CREATE = True

con = sqlite3.connect('movies.db')
con.row_factory = dict_factory

"""
I have opted for a set of genre options rather than a genres table.
This simplifies the schema a bit in the case that all we care about is
the genre name. If we cared, e.g., about genre hierarchy, we would need a
genres table.

Note that because the movie-genre relationship is many-to-many, there
is still a join table -- or a psuedo-join table, since it only links a
movie FK with a genre name.
"""
GENRES = (
    'Action',
    'Adult',
    'Adventure',
    'Animation',
    'Biography',
    'Comedy',
    'Crime',
    'Documentary',
    'Drama',
    'Family',
    'Fantasy',
    'Film-Noir',
    'Game-Show',
    'History',
    'Horror',
    'Music',
    'Musical',
    'Mystery',
    'News',
    'Reality-TV',
    'Romance',
    'Sci-Fi',
    'Short',
    'Sport',
    'Talk-Show',
    'Thriller',
    'War',
    'Western',
) # These are used for generating a check constraint in the
  # creation of the movie_genres table below 


def update_genres():
    """
    Most Dbs give us the ability to drop and re-create a check constraint
    with the ALTER TABLE syntax.  SQLite3 does not support this, thus
    we will rebuild the table."""
    c = con.cursor()
    c.execute("""CREATE TABLE movie_genres_tmp (
        movie INTEGER REFERENCES movies(imdbid),
        genre TEXT
        CONSTRAINT genre_check 
            CHECK (genre IN (%s)))""" % ','.join(
            "'%s'" % genre for genre in GENRES))
    c.execute("""INSERT INTO movie_genres_tmp (movie, genre)
        SELECT movie, genre FROM movie_genres""")
    c.execute("""DROP TABLE movie_genres""")
    c.execute("""ALTER TABLE movie_genres_tmp RENAME TO movie_genres""")


def create_movies_table():
    c = con.cursor()
    try:
        c.execute("""CREATE TABLE movies (
            imdbid INTEGER PRIMARY KEY,
            rtid INTEGER UNIQUE,
            title TEXT,
            year INTEGER,
            released TEXT,
            mpaa_rating TEXT,
            runtime INTEGER)
        """)
    except sqlite3.OperationalError, e:
        if e.message != 'table movies already exists':
            raise e
    con.commit()


def create_people_table():
    c = con.cursor()
    try:
        c.execute("""CREATE TABLE people (
            id INTEGER PRIMARY KEY,
            rtid INTEGER UNIQUE,
            name TEXT UNIQUE)""") # is this a good idea?
    except sqlite3.OperationalError, e:
        if e.message != 'table people already exists':
            raise e
    con.commit()


def create_movie_people_table():
    c = con.cursor()
    try:
        c.execute("""CREATE TABLE movie_people (
            movie INTEGER REFERENCES movies(imdbid),
            person INTEGER REFERENCES people(id),
            role TEXT,
            descr TEXT,
            CONSTRAINT movie_person_role_unique UNIQUE (movie, person, role))
        """)
    except sqlite3.OperationalError, e:
        if e.message != 'table movie_people already exists':
            raise e
    con.commit()


def create_movie_genres_table():
    c = con.cursor()
    try:
        c.execute("""CREATE TABLE movie_genres (
            movie INTEGER REFERENCES movies(imdbid),
            genre TEXT
            CONSTRAINT genre_check 
                CHECK (genre IN (%s)))""" % ','.join(
                "'%s'" % genre for genre in GENRES))
    except sqlite3.OperationalError, e:
        if e.message != 'table movie_genres already exists':
            raise e
    con.commit()
    

if CREATE:
    create_movies_table()
    create_people_table()
    create_movie_people_table()
    create_movie_genres_table()
    

def omdb_data(imdbid):
    url = OMDBAPI_URL % ('tt%s' % imdbid)
    r = urllib2.urlopen(url)
    data = json.loads(r.read())
    if data['Response'] == 'True':
        return data
    else:
        print 'OMDB Object not found:', imdbid
        return {}


def get_or_create_person(name, rtid=None):
    c = con.cursor()
    c.execute('SELECT id, rtid, name  FROM PEOPLE WHERE name=?', (name,))
    row = c.fetchone()
    if row:
        if rtid and row['rtid'] and int(rtid) != row['rtid']:
            raise Exception(
                'Ambiguous person %s with Rotten Tomatoes IDs: %s and %s'%(
                name, rtid, row['rtid']))
        return row
    else:
        print 'Inserting person:', name
        c.execute("""INSERT INTO PEOPLE (rtid, name) VALUES (?, ?)""",
            (rtid, name,))
        con.commit()
        c.execute('SELECT * FROM PEOPLE WHERE id=?', (c.lastrowid,))
        return c.fetchone()


def rotten_tomatoes_search(query):
    page_limit = 50
    page = 1
    params = {
        'q': query,
        'page_limit': page_limit,
        'page': page,
        'apikey': RTAPI_KEY,
    }
    url = '%s?%s' % (RTAPI_URL, urllib.urlencode(params.items()))
    r = urllib2.urlopen(url).read()
    data = json.loads(r)
    return data

def get_or_create_movie(movie):
    """
    Other data available in Rotten Tomatoes movie results:
        critics_consensus
        release_dates
        ratings
        synopsis
        posters
        alternate_ids
        links
    """
    created = False
    c = con.cursor()
    try:
        c.execute("""INSERT INTO MOVIES (
            imdbid, rtid, title, year, mpaa_rating, runtime) VALUES (
            ?, ?, ?, ?, ?, ?)""", (
            movie['alternate_ids']['imdb'],
            movie['id'],
            movie['title'],
            movie['year'],
            movie['mpaa_rating'],
            movie['runtime']))
        con.commit()
        created = True
    except sqlite3.IntegrityError, e:
        if e.message == 'datatype mismatch':
            print 'SKIPPING MOVIE. BAD DATA:', movie['title']
        elif e.message != 'UNIQUE constraint failed: movies.imdbid':
            raise e
    c.execute("""SELECT * FROM movies WHERE imdbid=?""", (
        movie['alternate_ids']['imdb'],))
    return c.fetchone(), created


def extract_genres(imdbid, omdb):
    genres = [genre.strip() for genre in omdb.get('Genre','').split(',') if
        genre != 'N/A' if genre.strip()] 
    for genre in genres:
        c = con.cursor()
        try:
            c.execute('INSERT INTO movie_genres (movie, genre) VALUES (?,?)',
                (imdbid, genre))
        except sqlite3.IntegrityError, e:
            """Here is a trick to tease out the range of options for a
            parameter when the documentation does not state explicitly.
            It is tedious, and you want to be careful about putting
            something like this into production, but during development
            this approach lets you move forward and fill in genres as
            you discover them.

            Note: you need to regenerate this schema if you change the
            genre list, thus making this even more painful. The reality is
            that you probably would not normally take this approach for
            data you don't have control over -- a genres table would be fine.
            """
            if e.message == 'CHECK constraint failed: genre_check':
                raise Exception('Invalid genre: %s' % genre)

def extract_directors(imdbid, omdb):
    for director in omdb.get('Director', '').split(','):
        director = director.strip()
        director_id = get_or_create_person(director)['id']
        c = con.cursor()
        try:
            c.execute("""INSERT INTO movie_people (movie, person, role)
                VALUES (?,?,?)""", (imdbid, director_id, 'director'))
            con.commit() 
        except sqlite3.IntegrityError, e:
            if e.message != 'UNIQUE constraint failed: movie_people.movie, movie_people.person, movie_people.role':
                raise e


def extract_writers(imdbid, omdb):
    omdb_writers = {}
    for writer in omdb.get('Writer', '').split(','):
        writer = writer.strip()
        descr = PAREN.findall(writer)
        writer = re.sub(PAREN, '', writer).strip()
        if writer not in omdb_writers:
            omdb_writers[writer] = []
        omdb_writers[writer].extend(descr)
    for writer, descr in omdb_writers.items():
        writer_id = get_or_create_person(writer)['id']
        descr = ';'.join(descr)
        c = con.cursor()
        c.execute("""INSERT INTO movie_people (movie, person, role, descr)
            VALUES (?,?,?,?)""", (imdbid, writer_id, 'writer', descr))
        con.commit() 


def extract_actors(imdbid, movie, omdb):
    actors = []
    c = con.cursor()
    for actor in movie['abridged_cast']:
        id_ = get_or_create_person(actor['name'], actor['id'])['id']
        c.execute("""INSERT INTO movie_people (movie, person, role, descr)
            VALUES (?,?,?,?)""", (imdbid, id_, 'actor',
            '; '.join(actor.get('characters', []))))
        con.commit() 
        actors.append(actor['name'])
    for actor in omdb.get('Actors', '').split(','):
        actor = actor.strip()
        if actor not in actors:
            id_ = get_or_create_person(actor)['id']
            try:
                c.execute("""INSERT INTO movie_people (movie, person, role)
                    VALUES (?,?,?)""", (imdbid, id_, 'actor'))
                con.commit() 
            except sqlite3.IntegrityError, e:
                if e.message != 'UNIQUE constraint failed: movie_people.movie, movie_people.person, movie_people.role':
                    raise e


def process_movies(data):
    for movie in data.get('movies', []):
        if not 'imdb' in movie.get('alternate_ids', {}):
            continue
        imdbid = movie['alternate_ids']['imdb']
        movie_obj, created = get_or_create_movie(movie)
        if not created:
            continue
        print 'NEW MOVIE:', movie['title'] 
        omdb = omdb_data(imdbid)
        extract_genres(imdbid, omdb)
        extract_directors(imdbid, omdb)
        extract_writers(imdbid, omdb)
        extract_actors(imdbid, movie, omdb)
    if data.get('links', {}).get('next'):
        url = data['links']['next'] + '&apikey=' + RTAPI_KEY
        r = urllib2.urlopen(url)
        process_movies(json.loads(r.read()))


def fetch_movies(query): 
    data = rotten_tomatoes_search(query)
    process_movies(data)


def movie_queries(queries):
    for query in queries:
        print 'QUERY:', query
        fetch_movies(query)


if __name__=='__main__':
    update_genres()
    # let's grab a whole bunch of movies from Rotten Tomatoes and
    # supplement with info from omdb
    movie_queries(
        ['1','2','3','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'])


"""
Some queries we can do on the data:

# all the movies:
SELECT COUNT(*) FROM movies;

# all the people
SELECT COUNT(*) FROM people;

# a specific movie
SELECT * FROM movies WHERE title = 'Back to the Future';

# specific movies with similar titles
SELECT * FROM movies WHERE title LIKE 'Back to the Future%';

# all the people in all the movies and their roles
SELECT * FROM movies m, people p, movie_people mp WHERE m.imdbid=mp.movie and p.id=mp.person;

# all the genres
SELECT DISTINCT genre FROM movie_genres ORDER BY genre;

# all Horror Musicals
SELECT m.title FROM movies m, movie_genres mg WHERE m.imdbid=mg.movie AND mg.genre IN ('Horror', 'Musical') GROUP BY m.imdbid HAVING COUNT(*)=2 ORDER BY m.title;

# a little better approach -- this uses a self join
select h.title from movies h, movie_genres hg, movies m, movie_genres mg where h.imdbid = m.imdbid and h.imdbid = hg.movie and hg.genre = 'Horror' and m.imdbid = mg.movie and mg.genre = 'Musical';

# directors working in the 80s and their movie titles
SELECT p.name, m.title FROM movies m, people p, movie_people mp where m.imdbid=mp.movie AND p.id=mp.person AND mp.role='director' AND m.year > 1979 AND m.year < 1990 ORDER BY p.name;

# average movie runtime by year
SELECT year, AVG(runtime) FROM movies GROUP BY year;

# number of movies we have for each year
SELECT year, count(*) FROM movies GROUP BY year;

# Kevin Bacon's inner circle
SELECT DISTINCT p.name, m.title FROM movies m, people p, movie_people mp WHERE m.imdbid=mp.movie AND p.id=mp.person AND m.imdbid IN (SELECT msub.imdbid FROM movies msub, people psub, movie_people mpsub WHERE msub.imdbid=mpsub.movie AND psub.id=mpsub.person AND psub.name='Kevin Bacon' AND mpsub.role='actor') AND p.name != 'Kevin Bacon';
"""
