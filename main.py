from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT
import csv
import sys
import pandas as pd


def main() -> None:
    # check that csv arg passed
    if len(sys.argv) != 2:
        print("Usage: main.py <csv file>")
        return
    # convert .xlsx to .csv and generate .csv file
    read_file = pd.read_excel(sys.argv[1])
    read_file.to_csv("test.csv", index=None, header=True)
    csv_name = sys.argv[1].replace(".xlsx", ".csv")
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
                            match key:
                                case "date_arrived":
                                    cur.execute(
                                        """INSERT INTO order_information (date_arrived) VALUES (%s)""",
                                        [value],
                                    )
                                case "date_ordered":
                                    cur.execute(
                                        """INSERT INTO order_information (date_ordered) VALUES (%s)""",
                                        [value],
                                    )
                                case "date_requested":
                                    cur.execute(
                                        """INSERT INTO order_information (date_requested) VALUES (%s)""",
                                        [value],
                                    )
                                case "list_price":
                                    cur.execute(
                                        """INSERT INTO reagent_information (list_price) VALUES (%s)""",
                                        [value],
                                    )
                                case "new_item":
                                    cur.execute(
                                        """INSERT INTO reagent_information (new_item) VALUES (%s)""",
                                        [value],
                                    )
                                case "po_number":
                                    cur.execute(
                                        """INSERT INTO order_information (po_number) VALUES (%s)""",
                                        [value],
                                    )
                                case "project_code":
                                    cur.execute(
                                        """INSERT INTO order_information (project_code) VALUES (%s)""",
                                        [value],
                                    )
                                case "quantity":
                                    cur.execute(
                                        """INSERT INTO reagent_information (quantity) VALUES (%s)""",
                                        [value],
                                    )
                                case "reagent":
                                    cur.execute(
                                        """INSERT INTO reagent_information (reagent) VALUES (%s)""",
                                        [value],
                                    )
                                case "researcher":
                                    cur.execute(
                                        """INSERT INTO order_information (researcher) VALUES (%s)""",
                                        [value],
                                    )
                                case "supplier":
                                    cur.execute(
                                        """INSERT INTO reagent_information (supplier) VALUES (%s)""",
                                        [value],
                                    )
                            conn.commit()
                except (Exception, DatabaseError) as error:
                    print(error)


if __name__ == "__main__":
    main()
