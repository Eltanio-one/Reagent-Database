from psycopg2 import connect, DatabaseError
from keys import HOST, USER, PASSWORD, DBNAME, PORT
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import io


def format_date(df, column):
    """Format dates into datetime object %Y-%m-%d"""
    return pd.to_datetime(df[column], format="mixed")


def main() -> None:
    """Process and import ordeering data into database"""
    if len(sys.argv) != 2:
        print("Usage: main.py <csv file>")
        return
    order_info = pd.read_csv(sys.argv[1])
    # rename columns for export to database
    order_info.rename(
        columns={
            "Reagent": "reagent_name",
            "New item?": "new_item",
            "Product Code": "product_code",
            "Supplier": "supplier",
            "Quantity": "quantity",
            "List Price": "list_price",
            "Date Requested": "date_requested",
            "Date Ordered": "date_ordered",
            "Date Arrived": "date_arrived",
            "Name": "researcher",
            "Project code": "project_code",
            "PO Number": "po_number",
        },
        inplace=True,
    )
    # calculate missing values
    missing_values = order_info.isnull().sum()

    # calculate percentage of data missing
    total_cells = np.product(order_info.shape)
    total_missing = missing_values.sum()
    percentage_missing = (total_missing / total_cells) * 100
    print(f"{percentage_missing:.1f}% of data missing")

    # check for rows that have all enterable columns empty and drop them
    order_info_edit = order_info.dropna(
        axis=0, subset=["reagent_name", "product_code", "supplier", "quantity"]
    )
    print(f"Rows in original dataset: {order_info.shape[0]}")
    print(f"Rows in modified dataset: {order_info_edit.shape[0]}")
    missing_values = order_info_edit.isnull().sum()
    # print(missing_values)

    order_info_edit["date_arrived"].replace({"yes": "NaT"}, inplace=True)

    # clean date formatting to %Y-%m-%d and then to string for importing
    date_columns = ["date_requested", "date_ordered", "date_arrived"]
    for column in date_columns:
        order_info_edit[column] = format_date(order_info_edit, column)
        order_info_edit[column] = order_info_edit[column].astype(str)

    # fill columns NaNs with relevant values (if not recorded or existent)
    order_info_edit["new_item"].fillna("n", inplace=True)
    order_info_edit["new_item"].replace({"Y": "y"}, inplace=True)
    order_info_edit["new_item"].replace({"yes": "y"}, inplace=True)
    order_info_edit["list_price"].fillna("Not provided", inplace=True)
    order_info_edit["po_number"].fillna("Not provided", inplace=True)
    order_info_edit["date_arrived"].replace({"NaT": "Not recorded"}, inplace=True)
    order_info_edit["date_ordered"].replace({"NaT": "Not recorded"}, inplace=True)
    order_info_edit["date_requested"].replace({"NaT": "Not recorded"}, inplace=True)
    order_info_edit["list_price"].replace(
        to_replace="?", value="Not provided", inplace=True
    )

    engine = create_engine(
        f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    )

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
                # insert to reagent_information table
                cur.execute("""SELECT * FROM reagent_information LIMIT 0""")
                table_columns = [desc[0] for desc in cur.description]
                reagent_information = order_info_edit[
                    order_info_edit.columns.intersection(table_columns)
                ]
                reagent_information.to_sql(
                    "reagent_information", con=engine, if_exists="append", index=False
                )
                # insert to order_information table
                cur.execute("""SELECT * FROM order_information LIMIT 0""")
                table_columns = [desc[0] for desc in cur.description]
                order_information = order_info_edit[
                    order_info_edit.columns.intersection(table_columns)
                ]
                order_information.to_sql(
                    "order_information", con=engine, if_exists="append", index=False
                )
    except (Exception, DatabaseError) as error:
        print(error)

    print("\nImport Complete")


if __name__ == "__main__":
    main()
