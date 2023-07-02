from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT


def insert_many(query: str, arguments=None) -> None:
    try:
        # params = config()
        conn = connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            dbname=DBNAME,
            port=PORT,
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, arguments)
                conn.commit()
    except (Exception, DatabaseError) as error:
        print(error)
