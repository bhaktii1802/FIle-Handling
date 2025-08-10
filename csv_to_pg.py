import psycopg2
import csv
import os

# ==========================
#  CONFIGURATION
# ==========================
DB_NAME = "google_data"        # your database name
DB_USER = "postgres"        # your PostgreSQL username
DB_PASSWORD = "root"  # your PostgreSQL password
DB_HOST = "localhost"
DB_PORT = "5432"

# CSV file paths
CSV_FULL = r"C:\Users\DELL\Desktop\File Handling\googl_daily_prices_g.csv"
CSV_ABOVE200 = r"C:\Users\DELL\Desktop\File Handling\top_closing_above_200.csv"
CSV_TOP5 = r"C:\Users\DELL\Desktop\File Handling\top_5_closing_g.csv"


TABLE_FULL = "googl_daily_prices_g"
TABLE_ABOVE200 = "top_closing_above_200"
TABLE_TOP5 = "top_5_closing_g  "

# ==========================
#  FUNCTION TO INSERT CSV
# ==========================
def insert_csv(file_path, table_name):
    """Insert CSV data into the given PostgreSQL table."""
    
    if not os.path.exists(file_path):
        print(f"❌ Skipping {table_name}: CSV file not found at {file_path}")
        return

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        with open(file_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Convert volume to integer
                if "volume" in row and row["volume"]:
                    try:
                        row["volume"] = int(float(row["volume"]))
                    except ValueError:
                        row["volume"] = None  # keep NULL if invalid

                # Prepare and execute INSERT
                cur.execute(
                    f"""INSERT INTO {table_name} (date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                    (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'])
                )

        conn.commit()
        print(f"✅ Data inserted into {table_name} successfully!")

    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


# ==========================
#  RUN INSERTS
# ==========================
insert_csv(CSV_FULL, TABLE_FULL)
insert_csv(CSV_ABOVE200, TABLE_ABOVE200)
insert_csv(CSV_TOP5, TABLE_TOP5)
