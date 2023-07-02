# imports
from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT
import csv
import sys
import pandas as pd


# take in the csv, command line? convert from xlsx to csv
def main():
    csv_name = sys.argv[1]
    read_file = pd.read_excel(csv_name)
    read_file.to_csv("test.csv", index=None, header=True)
    csv_name = csv_name.replace(".xlsx", ".csv")
    with open(csv_name, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            sql_dict = row
            keys = sql_dict.keys()
            vals = sql_dict.values()
            temp = []
            for key in keys:
                key = key.replace(" ", "_").lower().strip("?")
                temp.append(key)
            outputs = dict(zip(temp, vals))
            print(outputs["date_ordered"])
            for key in outputs.keys():
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
                            match key:
                                case "date_arrived":
                                    cur.execute(
                                        """INSERT INTO date_arrived (date_arrived) VALUES (%s)""",
                                        (outputs["date_arrived"]),
                                    )
                                case "date_ordered":
                                    cur.execute(
                                        """INSERT INTO date_ordered (date_ordered) VALUES (%s)""",
                                        (outputs["date_ordered"]),
                                    )
                                case "date_requested":
                                    cur.execute(
                                        """INSERT INTO date_requested (date_requested) VALUES (%s)""",
                                        (outputs["date_requested"]),
                                    )
                                case "list_price":
                                    cur.execute(
                                        """INSERT INTO list_price (list_price) VALUES (%s)""",
                                        (outputs["list_price"]),
                                    )
                                case "new_item":
                                    cur.execute(
                                        """INSERT INTO new_item (new_item) VALUES (%s)""",
                                        (outputs["new_item"]),
                                    )
                                case "po_number":
                                    cur.execute(
                                        """INSERT INTO po_number (po_number) VALUES (%s)""",
                                        (outputs["po_number"]),
                                    )
                                case "project_code":
                                    cur.execute(
                                        """INSERT INTO project_code (project_code) VALUES (%s)""",
                                        (outputs["project_code"]),
                                    )
                                case "quantity":
                                    cur.execute(
                                        """INSERT INTO quantity (quantity) VALUES (%s)""",
                                        (outputs["quantity"]),
                                    )
                                case "reagent":
                                    cur.execute(
                                        """INSERT INTO reagent (reagent) VALUES (%s)""",
                                        (outputs["reagent"]),
                                    )
                                case "researcher":
                                    cur.execute(
                                        """INSERT INTO researcher (researcher) VALUES (%s)""",
                                        (outputs["researcher"]),
                                    )
                                case "supplier":
                                    cur.execute(
                                        """INSERT INTO supplier (supplier) VALUES (%s)""",
                                        (outputs["supplier"]),
                                    )

                            conn.commit()
                except (Exception, DatabaseError) as error:
                    print(error)


if __name__ == "__main__":
    main()
