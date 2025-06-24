import pandas as pd

def SQLQuery(cnxn: any, query: str):
    df = pd.read_sql(query, cnxn)

    if df.empty:
        print("No data returned from SQL query.")
    else:
        print(f"Data retrieved successfully with {len(df)} rows.")

    return df