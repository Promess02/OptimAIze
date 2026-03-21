import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import timedelta
from fastapi import FastAPI
import uvicorn

app = FastAPI(title="Supply Chain & Forecasting API")

def get_db_data():
    conn = sqlite3.connect('ecommerce.db')
    
    # 1. Wybieramy 10 topowych produktów z historii
    top_10_query = """
        SELECT product_id, SUM(sales) as total_sales 
        FROM sales_aggregated 
        GROUP BY product_id 
        ORDER BY total_sales DESC 
        LIMIT 10
    """
    top_products_df = pd.read_sql(top_10_query, conn)
    top_product_ids = top_products_df['product_id'].tolist()
    
    # Zabezpieczenie przed pustą listą
    if not top_product_ids:
        conn.close()
        return pd.DataFrame(), pd.DataFrame(), []
        
    placeholders = ','.join(['?'] * len(top_product_ids))
    
    # 2. Pobieramy historyczne dane sprzedaży tylko dla tych 10 produktów
    sales_query = f"SELECT * FROM sales_aggregated WHERE product_id IN ({placeholders})"
    df_sales = pd.read_sql(sales_query, conn, params=top_product_ids)
    df_sales['date'] = pd.to_datetime(df_sales['date'])
    
    # 3. Pobieramy ich stany magazynowe
    inv_query = f"SELECT * FROM inventory WHERE product_id IN ({placeholders})"
    df_inv = pd.read_sql(inv_query, conn, params=top_product_ids)
    
    conn.close()
    return df_sales, df_inv, top_product_ids

def extract_time_features(df, is_future=False, min_date=None):
    """Ekstrakcja cech czasowych dla XGBoost"""
    df = df.copy()
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day'] = df['date'].dt.day
    
    if not is_future:
        min_date = df['date'].min()
        
    df['days_since_start'] = (df['date'] - min_date).dt.days
    return df, min_date

@app.get("/api/v1/generate-order")
def generate_order():
    """Endpoint HTTP realizujący flow predykcji i zamówienia."""
    df_sales, df_inv, top_products = get_db_data()
    
    if df_sales.empty:
        return {"status": "error", "message": "Brak danych w bazie"}

    order_report = []

    # Iterujemy po każdym produkcie indywidualnie – trenujemy niezależny model popytu
    for pid in top_products:
        prod_sales = df_sales[df_sales['product_id'] == pid].copy()
        
        if len(prod_sales) < 5:
            continue # Produkt ma zbyt mało danych historycznych, pomijamy

        # 4. Inżynieria cech dla ML
        prod_sales, min_date = extract_time_features(prod_sales)
        X_train = prod_sales[['dayofweek', 'month', 'year', 'day', 'days_since_start']]
        y_train = prod_sales['sales']

        # 5. Trenowanie modelu XGBoost
        model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=50, random_state=42)
        model.fit(X_train, y_train)

        # 6. Prognoza na kolejne 2 miesiące (np. 60 dni)
        last_date = prod_sales['date'].max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, 61)]
        df_future = pd.DataFrame({'date': future_dates})
        
        df_future, _ = extract_time_features(df_future, is_future=True, min_date=min_date)
        X_future = df_future[['dayofweek', 'month', 'year', 'day', 'days_since_start']]
        
        # Predykcja (obcinamy wartości poniżej zera gdyby model "przestrzelił" w dół)
        preds = model.predict(X_future)
        preds = np.maximum(0, preds)
        
        predicted_demand_60_days = int(np.sum(preds))

        # 7. Porównanie ze stanem magazynowym
        stock_info = df_inv[df_inv['product_id'] == pid]['current_stock'].values
        current_stock = int(stock_info[0]) if len(stock_info) > 0 else 0

        # Obliczamy ile musimy dokupić, by pokryć popyt w 100%
        order_quantity = max(0, predicted_demand_60_days - current_stock)

        order_report.append({
            "product_id": pid,
            "predicted_demand_2_months": predicted_demand_60_days,
            "current_inventory": current_stock,
            "suggested_order_quantity": order_quantity
        })

    return {
        "status": "success",
        "processed_products": len(order_report),
        "orders_to_place": order_report
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
