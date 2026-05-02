import pandas as pd
import numpy as np

def generate_demand_forecast(df):
    """
    Train an XGBoost model on historical data and predict demand.
    Exported from sales_forecasting_models.ipynb logic.
    """
    df = df.copy()
    if len(df) < 10:
        df['predicted_demand'] = df['sales']
        return df

    # Feature engineering for ML Model
    df['dayofweek'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['sales_lag_1'] = df['sales'].shift(1).fillna(0)
    df['sales_lag_7'] = df['sales'].shift(7).fillna(0)
    df['sales_roll_7'] = df['sales'].rolling(window=7, min_periods=1).mean()
    
    # Target and Features
    target = 'sales'
    features = ['dayofweek', 'month', 'sales_lag_1', 'sales_lag_7', 'sales_roll_7']
    
    # Splitting to train on past and predict to mimic an out-of-sample or at least fitted baseline
    # For simulation baseline usage, we'll train on the whole series and provide in-sample + out-of-sample predictions
    # or just train on the first 70% and predict the rest. Using the whole history as baseline is a simpler proxy.
    X = df[features]
    y = df[target]

    import xgboost as xgb
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=100,
        max_depth=4,
        learning_rate=0.05,
        random_state=42
    )
    
    model.fit(X, y)
    df['predicted_demand'] = np.clip(model.predict(X), 0, None)
    
    return df

def predict_demand(model, current_features):
    """
    Predict demand using a pre-trained model (e.g., LightGBM, XGBoost, or LSTM).
    """
    # For tree-based models, features should be a 2D array or DataFrame
    predicted_demand = model.predict(current_features)
    # Ensure no negative demand
    return max(0.0, predicted_demand[0])

def adjust_price(current_price, predicted_demand, avg_demand):
    """
    Adjust the product price based on predicted demand relative to the average demand.
    """
    # Define thresholds for high and low demand (e.g., +/- 15% of average demand)
    high_demand_threshold = avg_demand * 1.15
    low_demand_threshold = avg_demand * 0.85
    
    if predicted_demand > high_demand_threshold:
        # High demand predicted: increase price by 5%
        new_price = current_price * 1.05
        print(f"High demand predicted ({predicted_demand:.2f} > {high_demand_threshold:.2f}). Increasing price to {new_price:.2f}.")
    elif predicted_demand < low_demand_threshold:
        # Low demand predicted: decrease price by 5% to stimulate demand
        new_price = current_price * 0.95
        print(f"Low demand predicted ({predicted_demand:.2f} < {low_demand_threshold:.2f}). Decreasing price to {new_price:.2f}.")
    else:
        # Normal demand predicted: keep price stable
        new_price = current_price
        print(f"Normal demand predicted ({predicted_demand:.2f}). Keeping price stable at {new_price:.2f}.")
        
    return new_price

def run_simulation(product_id, initial_data, model, feature_cols, steps=30):
    """
    Run the dynamic pricing simulation for a given number of steps.
    """
    print(f"Starting simulation for product {product_id}")
    history = initial_data.copy()
    avg_demand = history['sales'].mean()
    
    current_price = history.iloc[-1]['price']
    simulation_results = []
    
    for step in range(1, steps + 1):
        print(f"--- Step {step} ---")
        # 1. Get the latest features for the model from our history
        latest_features = history.iloc[[-1]][feature_cols]
        
        # 2. Predict demand for the next step
        predicted_demand = predict_demand(model, latest_features)
        
        # 3. Adjust price using the predicted demand
        new_price = adjust_price(current_price, predicted_demand, avg_demand)
        
        # 4. Log results
        simulation_results.append({
            'step': step,
            'product_id': product_id,
            'predicted_demand': predicted_demand,
            'old_price': current_price,
            'new_price': new_price
        })
        
        # 5. Update current state for the next iteration (simulating the time step advancing)
        current_price = new_price
        
    return pd.DataFrame(simulation_results)