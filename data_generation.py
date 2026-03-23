import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Generate 1 million rows ~ 150-200MB
n = 1_000_000

data = {
    'trip_id': range(1, n+1),
    'pickup_datetime': [datetime(2023,1,1) + timedelta(minutes=random.randint(0,525600)) for _ in range(n)],
    'dropoff_datetime': [datetime(2023,1,1) + timedelta(minutes=random.randint(0,525600)) for _ in range(n)],
    'passenger_count': np.random.randint(1, 7, n),
    'trip_distance': np.round(np.random.uniform(0.5, 30.0, n), 2),
    'pickup_location': np.random.choice(['Manhattan','Brooklyn','Queens','Bronx','Staten Island'], n),
    'dropoff_location': np.random.choice(['Manhattan','Brooklyn','Queens','Bronx','Staten Island'], n),
    'fare_amount': np.round(np.random.uniform(5.0, 150.0, n), 2),
    'tip_amount': np.round(np.random.uniform(0.0, 30.0, n), 2),
    'payment_type': np.random.choice(['Credit Card','Cash','No Charge','Dispute'], n),
    'vendor': np.random.choice(['VendorA','VendorB','VendorC'], n)
}

df = pd.DataFrame(data)
df.to_csv('taxi_data.csv', index=False)
print("Done!", df.shape)