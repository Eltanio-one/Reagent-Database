from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT


def create_tables():
    """initialise tables in postgresql database"""
    commands = (
        """
        CREATE TABLE order_information (
            order_id SERIAL PRIMARY KEY NOT NULL,
            date_requested VARCHAR(255) NOT NULL,
            date_ordered VARCHAR(255) NOT NULL,
            date_arrived VARCHAR(255) NOT NULL,
            po_number VARCHAR(255) NOT NULL,
        )
        """,
        """
        CREATE TABLE reagent_information (
            reagent_id SERIAL PRIMARY KEY NOT NULL,
            new_item VARCHAR(255) NOT NULL,
            product_code VARCHAR(255) NOT NULL,
            supplier VARCHAR(255) NOT NULL,
            researcher VARCHAR(255) NOT NULL,
            quantity VARCHAR(255) NOT NULL,
            FOREIGN KEY (reagent_id) REFERENCES order_information (order_id) ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)
    
    try:
        conn = connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            dbname=DBNAME,
            port=PORT,
        )
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, DatabaseError) as error:
        print(error)
    
if __name__ == "__main__":
    create_tables()