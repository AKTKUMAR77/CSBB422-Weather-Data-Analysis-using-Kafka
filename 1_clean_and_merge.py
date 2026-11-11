import os
import pandas as pd

print("Loading Excel datasets from 'dataset/' folder...")
DATASET_DIR = "dataset"

df_loc = pd.read_excel(os.path.join(DATASET_DIR, 'Location information.xlsx'))
df_weather = pd.read_excel(os.path.join(DATASET_DIR, 'Weather data.xlsx'))
df_astro = pd.read_excel(os.path.join(DATASET_DIR, 'Astronomical.xlsx'))
df_air = pd.read_excel(os.path.join(DATASET_DIR, 'Air quality information.xlsx'))

merged_df = df_loc.merge(df_weather, on='last_updated_epoch') \
                  .merge(df_astro, on='last_updated_epoch') \
                  .merge(df_air, on='last_updated_epoch')

merged_df.to_csv("indian_weather_master.csv", index=False)
print(f"Dataset clean and merged! Saved {merged_df.shape[0]} rows and {merged_df.shape[1]} columns to 'indian_weather_master.csv'.")