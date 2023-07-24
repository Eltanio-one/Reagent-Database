from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT


def create_tables():
    """initialise tables in postgresql database"""
    commands = (
        """
        CREATE TABLE reagent_information (
            reagent_id SERIAL PRIMARY KEY,
            reagent_name VARCHAR(255),
            new_item VARCHAR(255),
            product_code VARCHAR(255),
            supplier VARCHAR(255),
            quantity VARCHAR(255),
            list_price VARCHAR(255)
        )
        """,
        """
        CREATE TABLE order_information (
            order_id SERIAL PRIMARY KEY,
            date_requested VARCHAR(255),
            date_ordered VARCHAR(255),
            date_arrived VARCHAR(255),
            researcher VARCHAR(255),
            reagent_name VARCHAR(255),
            product_code VARCHAR(255),
            project_code VARCHAR(255),
            po_number VARCHAR(255)
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