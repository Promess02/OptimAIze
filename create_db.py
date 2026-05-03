import pandas as pd
import sqlite3
import os

# Wczytanie danych o sprzedaży (zakładamy, że z uwagi na rozmiar, wczytujemy istotne kolumny)
if os.path.exists('sales.csv'):
    df_sales = pd.read_csv('sales.csv', usecols=["product_id", "date", "sales", "revenue", "price", "stock"])
else:
    print("sales.csv not found. Creating sample data...")
    # Create sample data for testing
    import numpy as np
    from datetime import datetime, timedelta

    np.random.seed(42)
    dates = [datetime.now() - timedelta(days=x) for x in range(90, 0, -1)]
    products = [f"PROD{str(i).zfill(3)}" for i in range(1, 11)]

    data = []
    for date in dates:
        for product in products:
            data.append({
                'product_id': product,
                'date': date.strftime("%Y-%m-%d"),
                'sales': np.random.randint(10, 100),
                'revenue': np.random.uniform(500, 5000),
                'price': np.random.uniform(50, 200),
                'stock': np.random.randint(100, 1000)
            })

    df_sales = pd.DataFrame(data)
    print(f"Created sample data with {len(df_sales)} records")

# Ensure date column is in datetime format
df_sales['date'] = pd.to_datetime(df_sales['date'])

# 1. Agregacja danych o sprzedaży
agg_sales = df_sales.groupby(['product_id', 'date']).agg({
    'sales': 'sum',
    'revenue': 'sum',
    'price': 'mean',
    'stock': 'sum'
}).reset_index()

# Ensure the database file is writable (fixes Docker root ownership issue)
db_file = 'ecommerce.db'
if os.path.exists(db_file) and not os.access(db_file, os.W_OK):
    raise PermissionError(
        f"Cannot write to '{db_file}'. It may be owned by root. "
        f"Run 'sudo chown $USER:$USER {db_file}' in your terminal to fix this."
    )

# Podłączenie do bazy SQLite
conn = sqlite3.connect(db_file, timeout=30.0)
cursor = conn.cursor()

# Zapis zagregowanych sprzedaży do tabeli 'sales_aggregated'
agg_sales.to_sql('sales_aggregated', conn, if_exists='replace', index=False)
print("✓ Dane sprzedaży zostały zagregowane i zapisane w tabeli 'sales_aggregated'.")

# 2. Generowanie tabeli z produktami i stanami magazynowymi
# Wyciągamy najnowszy stan magazynowy oraz średnią cenę
inventory_df = df_sales.sort_values('date').groupby('product_id').agg({
    'stock': 'last',
    'price': 'mean'
}).reset_index()
inventory_df.rename(columns={'stock': 'current_stock', 'price': 'price'}, inplace=True)
inventory_df['current_stock'] = inventory_df['current_stock'].fillna(0).astype(int)
inventory_df['price'] = inventory_df['price'].fillna(99.99).astype(float).round(2)

# Zapis do bazy danych
inventory_df.to_sql('inventory', conn, if_exists='replace', index=False)
print("✓ Stany magazynowe i ceny zostały zapisane w tabeli 'inventory'.")

# 3. Verify table schema
print("\n" + "="*60)
print("DATABASE SCHEMA VERIFICATION")
print("="*60)

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"\nTables created: {[t[0] for t in tables]}")

# Display inventory table structure
print("\nInventory table structure:")
cursor.execute("PRAGMA table_info(inventory);")
columns = cursor.fetchall()
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

# Display sample inventory data
print("\nSample inventory data:")
inv_sample = pd.read_sql("SELECT * FROM inventory LIMIT 3", conn)
print(inv_sample.to_string())

# Display sales_aggregated table structure
print("\nSales_aggregated table structure:")
cursor.execute("PRAGMA table_info(sales_aggregated);")
columns = cursor.fetchall()
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

print("\n" + "="*60)
print(f"✓ Database initialized successfully!")
print(f"  Total products: {len(inventory_df)}")
print(f"  Total sales records: {len(agg_sales)}")
print("="*60 + "\n")

conn.close()