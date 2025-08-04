# load_data.py

import sqlite3
import csv
import os

# --- Configuration ---
DB_FILE = 'ecommerce.db'
USERS_CSV = 'users.csv'
ORDERS_CSV = 'orders.csv'

# --- Database Initialization and Table Creation ---
def create_tables(cursor):
    """
    Reads the schema.sql file and executes the CREATE TABLE statements.
    """
    try:
        with open('schema.sql', 'r') as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        print("Database tables created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")

# --- Data Loading Functions ---
def load_users(cursor, conn):
    """
    Reads users.csv and inserts data into the users table.
    Handles unique constraint errors by skipping duplicate rows.
    """
    rows_loaded = 0
    rows_skipped = 0
    try:
        with open(USERS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            insert_query = """
            INSERT INTO users (
                id, first_name, last_name, email, age, gender, state,
                street_address, postal_code, city, country, latitude,
                longitude, traffic_source, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for row in reader:
                try:
                    cursor.execute(insert_query, (
                        row['id'], row['first_name'], row['last_name'], row['email'],
                        row['age'], row['gender'], row['state'], row['street_address'],
                        row['postal_code'], row['city'], row['country'],
                        row.get('latitude'), row.get('longitude'), 
                        row['traffic_source'], row['created_at']
                    ))
                    rows_loaded += 1
                except sqlite3.IntegrityError as e:
                    # This handles the UNIQUE constraint error for duplicate emails
                    if "UNIQUE constraint failed: users.email" in str(e):
                        rows_skipped += 1
                    else:
                        print(f"Error inserting row: {e}")
                        
            # After the loop, commit the successful insertions
            conn.commit()
            print(f"Loaded {rows_loaded} users into the database. Skipped {rows_skipped} rows due to duplicate emails.")
            
    except FileNotFoundError:
        print(f"Error: {USERS_CSV} not found.")
    except Exception as e:
        print(f"Error loading users data: {e}")

def load_orders(cursor, conn):
    """
    Reads orders.csv and inserts data into the orders table.
    """
    rows_loaded = 0
    try:
        with open(ORDERS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            insert_query = """
            INSERT INTO orders (
                order_id, user_id, status, gender, created_at,
                returned_at, shipped_at, delivered_at, num_of_item
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            data_to_insert = []
            for row in reader:
                data_to_insert.append((
                    row['order_id'], row['user_id'], row['status'],
                    row['gender'], row['created_at'], row['returned_at'],
                    row['shipped_at'], row['delivered_at'], row['num_of_item']
                ))
            
            cursor.executemany(insert_query, data_to_insert)
            rows_loaded = len(data_to_insert)
            conn.commit()
            print(f"Loaded {rows_loaded} orders into the database.")
            
    except FileNotFoundError:
        print(f"Error: {ORDERS_CSV} not found.")
    except Exception as e:
        print(f"Error loading orders data: {e}")

# --- Main Execution Block ---
def main():
    # Delete the existing database file to start fresh
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        create_tables(cursor)
        load_users(cursor, conn)
        load_orders(cursor, conn)
        
        print("Data loading process complete.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()