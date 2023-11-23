import sqlite3 as db

connection = db.connect('data/tutorial.db')
cursor = connection.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS movie(title, year, score)")
cursor.execute("""
    INSERT INTO movie VALUES
        ('Monty Python and the Holy Grail', 1975, 8.2),
        ('And Now for Something Completely Different', 1971, 7.5)
""")
connection.commit()

data = [
    ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
    ("Monty Python's The Meaning of Life", 1983, 7.5),
    ("Monty Python's Life of Brian", 1979, 8.0),
]
cursor.executemany('INSERT INTO movie VALUES(?, ?, ?)', data)
connection.commit()

connection.close()
