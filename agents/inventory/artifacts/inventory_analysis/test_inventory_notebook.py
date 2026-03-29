
import numpy as np
import pandas as pd

def test_abc_split_has_valid_labels():
    df = pd.DataFrame({'product_id':['a','b','c'], 'annual_revenue':[100,50,10], 'demand_mean_daily':[1,1,1], 'demand_std_daily':[0.1,0.2,0.3], 'cv':[0.1,0.2,0.3]})
    out = classify_abc(df)
    assert set(out['abc_class']).issubset({'A','B','C'})

def test_xyz_split_has_valid_labels():
    df = pd.DataFrame({'abc_class':['A','B','C'], 'cv':[0.3,0.8,1.4]})
    out = classify_xyz(df)
    assert list(out['xyz_class']) == ['X','Y','Z']

def test_reorder_qty_non_negative():
    local = result_df.copy()
    assert (local['reorder_qty'] >= 0).all()
