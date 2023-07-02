from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT


def create_tables():
    """initialise tables in postgresql database"""
    commands = (
        """
        CREATE TABLE administrator (
            id SERIAL PRIMARY KEY NOT NULL,
            user_id INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE date_requested (
            id SERIAL PRIMARY KEY NOT NULL,
            date_requested VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE date_ordered (
            id SERIAL PRIMARY KEY NOT NULL,
            date_ordered VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE new_item (
            id SERIAL PRIMARY KEY NOT NULL,
            new_item VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE project_code (
            id SERIAL PRIMARY KEY NOT NULL,
            project_code VARCHAR(255) NOT NULL 
        )
        """,
        """
        CREATE TABLE researcher (
            id SERIAL PRIMARY KEY NOT NULL,
            researcher VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE reagent (
            id SERIAL PRIMARY KEY NOT NULL,
            reagent VARCHAR(255) NOT NULL 
        )
        """,
        """
        CREATE TABLE quantity (
            id SERIAL PRIMARY KEY NOT NULL,
            quantity VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE product_code (
            id SERIAL PRIMARY KEY NOT NULL,
            product_code VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE supplier (
            id SERIAL PRIMARY KEY NOT NULL,
            supplier VARCHAR(255) NOT NULL  
        )
        """,
        """
        CREATE TABLE po_number (
            id SERIAL PRIMARY KEY NOT NULL,
            po_number VARCHAR(255) NOT NULL  
        )
        """,
        """
        CREATE TABLE date_arrived (
            id SERIAL PRIMARY KEY NOT NULL,
            date_arrived VARCHAR(255) NOT NULL  
        )
        """,
        """ 
        CREATE TABLE list_price (
            id SERIAL PRIMARY KEY NOT NULL,
            list_price VARCHAR(255) NOT NULL
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