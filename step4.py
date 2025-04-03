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

def list_tables(db_file):
    """List all tables in the SQLite database."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]

def interactive_cli():
    """Interactive CLI for user interaction."""
    db_file = "database.db"
    while True:
        print("\nOptions:")
        print("1. Load CSV into Database")
        print("2. List Tables")
        print("3. Run SQL Query")
        print("4. Exit")
        choice = input("Enter your choice: ").strip()
        
        if choice == "1":
            csv_file = input("Enter CSV file path: ").strip()
            table_name = input("Enter table name: ").strip()
            create_table_from_csv(csv_file, db_file, table_name)
        elif choice == "2":
            tables = list_tables(db_file)
            print("Tables in database:", tables)
        elif choice == "3":
            query = input("Enter SQL query: ").strip()
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                print("Query result:", cursor.fetchall())
            except Exception as e:
                print("Error executing query:", e)
            conn.close()
        elif choice == "4":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    interactive_cli()
