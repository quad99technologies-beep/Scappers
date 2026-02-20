
import sys
sys.path.insert(0, 'd:/quad99/Scrappers')
from core.db.postgres_connection import COUNTRY_PREFIX_MAP, PostgresDB

print('Italy prefix:', COUNTRY_PREFIX_MAP.get('Italy', 'NOT FOUND'))
db = PostgresDB('Italy')
print('db._prefix:', db._prefix)

db.connect()
cur = db.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = 'public' "
    "ORDER BY table_name"
)
rows = cur.fetchall()
print('it_ tables in DB:', [r[0] for r in rows])
db.close()
