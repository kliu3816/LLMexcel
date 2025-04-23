import pandas as pd
import sqlite3
import logging
import openai
import os
import re

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

def extract_sql_from_response(response_text):
    """Extract SQL from a mixed AI response using regex."""
    match = re.search(r"(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|WITH)\b.*", response_text, re.IGNORECASE | re.DOTALL)
    return match.group(0).strip() if match else response_text.strip()

def generate_sql_from_prompt(prompt, schema):
    """Use an LLM to generate SQL queries based on a natural language prompt."""
    api_key = os.getenv("OPENAI_API_KEY")
    openai.api_key = api_key
    if not openai.api_key:
        raise ValueError("OpenAI API key not found. Please set the OPENAI_API_KEY environment variable.")
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {
                "role": "system",
                "content": "You are an AI that outputs only SQL queries. Return only valid SQL statements, with no explanations or comments."
            },
            {
                "role": "user",
                "content": f"Database Schema: {schema}. User Request: {prompt}"
            }
        ]
    )
    response_text = response['choices'][0]['message']['content']
    return extract_sql_from_response(response_text)

def interactive_cli():
    """Interactive CLI for user interaction."""
    db_file = "database.db"
    while True:
        print("\nOptions:")
        print("1. Load CSV into Database")
        print("2. List Tables")
        print("3. Run SQL Query")
        print("4. Ask AI to Generate SQL")
        print("5. Exit")
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
            prompt = input("Enter your request in plain language: ").strip()
            conn = sqlite3.connect(db_file)
            tables = list_tables(db_file)
            schema = {table: get_existing_schema(conn, table) for table in tables}
            conn.close()
            sql_query = generate_sql_from_prompt(prompt, schema)
            print("Generated SQL:", sql_query)
            interactive_execution = input("Do you want to execute this SQL? (Y/N): ").strip().upper()
            if interactive_execution == "Y":
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                try:
                    cursor.execute(sql_query)
                    print("Query result:", cursor.fetchall())
                except Exception as e:
                    print("Error executing AI-generated query:", e)
                conn.close()
        elif choice == "5":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    interactive_cli()
