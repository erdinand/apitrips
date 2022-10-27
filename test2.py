import pandas as pd
import os

df = pd.read_csv('source/trips.csv')
new_csv = "source/trips_100M.csv"
df.to_csv(new_csv, mode='a', index=False, header=not os.path.exists(new_csv))
n_rows = 1000000
for i in range(1,n_rows):
    df.to_csv(new_csv, mode='a', header=False, index=False)
    if i in (100000,200000,300000,400000,500000,600000,700000,800000,900000,999999):
        print(f"Wrote {i} of {n_rows}")
