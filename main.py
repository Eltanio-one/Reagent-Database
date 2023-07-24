from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT
import csv
import sys


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: main.py <csv file>")
        return
    with open(sys.argv[1], "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            keys = row.keys()
            vals = row.values()
            temp = []
            for key in keys:
                key = key.replace(" ", "_").lower().strip("?")
                temp.append(key)
            outputs = dict(zip(temp, vals))
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
                        cur.execute(
                            """INSERT INTO reagent_information (reagent_name, new_item, product_code, supplier, quantity, list_price) VALUES (%s, %s, %s, %s, %s, %s)""",
                            (
                                outputs["reagent"],
                                outputs["new_item"],
                                outputs["product_code"],
                                outputs["supplier"],
                                outputs["quantity"],
                                outputs["list_price"],
                            ),
                        )
                        cur.execute(
                            """INSERT INTO order_information (reagent_name, date_requested, date_ordered, date_arrived, researcher, project_code, po_number, product_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (
                                outputs["reagent"],
                                outputs["date_requested"],
                                outputs["date_ordered"],
                                outputs["date_arrived"],
                                outputs["name"],
                                outputs["project_code"],
                                outputs["po_number"],
                                outputs["product_code"],
                            ),
                        )
                        conn.commit()
            except (Exception, DatabaseError) as error:
                print(error)


if __name__ == "__main__":
    main()
