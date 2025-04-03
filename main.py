import pandas as pd
import sqlite3
import logging

def infer_sqlite_type(dtype):
    """Map pandas data types to SQLite types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"

def get_existing_schema(conn, table_name):
    """Retrieve existing table schema using PRAGMA."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return {row[1]: row[2] for row in cursor.fetchall()}  # {column_name: data_type}

def handle_schema_conflict(existing_schema, new_schema):
    """Prompt user on schema conflict: overwrite, rename, or skip."""
    print("Schema conflict detected!")
    print("Existing Schema:", existing_schema)
    print("New Schema:", new_schema)
    choice = input("Choose an option - Overwrite (O), Rename Table (R), Skip (S): ").strip().upper()
    return choice

def create_table_from_csv(csv_file, db_file, table_name):
    logging.basicConfig(filename="error_log.txt", level=logging.ERROR)
    conn = sqlite3.connect(db_file)
    df = pd.read_csv(csv_file)
    
    # Infer column types
    new_schema = {col: infer_sqlite_type(df[col]) for col in df.columns}
    
    existing_schema = get_existing_schema(conn, table_name)
    if existing_schema:
        choice = handle_schema_conflict(existing_schema, new_schema)
        if choice == "S":
            print(f"Skipping table '{table_name}'...")
            conn.close()
            return
        elif choice == "R":
            table_name += "_new"
        elif choice == "O":
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            conn.commit()
    
    # Create table dynamically
    columns = [f'"{col}" {dtype}' for col, dtype in new_schema.items()]
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)});'
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Table '{table_name}' created and data inserted successfully!")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        print(f"An error occurred. Check error_log.txt for details.")
    finally:
        conn.close()

def run_queries(db_file, table_name):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
    print("First 5 Records:", cursor.fetchall())
    
    conn.close()

if __name__ == "__main__":
    csv_file = "people.csv"  # Change this to your actual CSV file path
    db_file = "database.db"  # SQLite database file
    table_name = "data_table"
    
    create_table_from_csv(csv_file, db_file, table_name)
    run_queries(db_file, table_name)
