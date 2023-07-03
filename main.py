from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT
import csv
import sys
import pandas as pd


def main():
    # check that csv arg passed
    if len(sys.argv) != 2:
        print("Usage: main.py <csv file>")
        return
    # convert .xlsx to .csv
    csv_name = sys.argv[1]
    read_file = pd.read_excel(csv_name)
    read_file.to_csv("test.csv", index=None, header=True)
    csv_name = csv_name.replace(".xlsx", ".csv")
    # read csv
    with open(csv_name, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # define total keys and total values variables
            keys = row.keys()
            vals = row.values()
            temp = []
            for key in keys:
                # strip of whitespaces and ? punctuation from original headers
                key = key.replace(" ", "_").lower().strip("?")
                # create list of keys to create new dictionary with update keys
                temp.append(key)
            # create new dict of updated keys and original values
            outputs = dict(zip(temp, vals))
            for key, value in outputs.items():
                # initialise connection to database
                try:
                    conn = connect(
                        host=HOST,
                        user=USER,
                        password=PASSWORD,
                        dbname=DBNAME,
                        port=PORT,
                    )
                    with conn:
                        with conn.cursor() as cur:
                            # match keys to perform specific insert operation
                            if key in [
                                "date_arrived",
                                "date_ordered",
                                "date_received",
                                "po_number",
                            ]:
                                cur.execute(
                                    """INSERT INTO order_information (%s) VALUES (%s)""",
                                    (key, value),
                                )
                            else:
                                cur.execute(
                                    """INSERT INTO reagent_information (%s) VALUES (%s)""",
                                    (key, value),
                                )
                            conn.commit()
                except (Exception, DatabaseError) as error:
                    print(error)


if __name__ == "__main__":
    main()
