import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def log(message):
    print(f"[LOG]: {message}")

def extract():
    url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    tables = soup.find_all('table', {'class': 'wikitable'})
    
    # Choose the correct table (you can adjust if Wikipedia changes it)
    df = pd.read_html(str(tables[0]))[0]
    
    log("Extracted Table Columns:")
    print(df.columns)

    # Rename and clean column names to standard
    df.rename(columns={
        df.columns[0]: "Rank",
        df.columns[1]: "Bank_Name",
        df.columns[2]: "Total_Assets_USD_Billion"
    }, inplace=True)

    # Keep only required columns
    df = df[["Bank_Name", "Total_Assets_USD_Billion"]]

    return df

def transform(df):
    # Clean numeric column: remove commas, convert to float
    df["Total_Assets_USD_Billion"] = (
        df["Total_Assets_USD_Billion"]
        .replace('[\$,]', '', regex=True)
        .replace('—', '0')  # Handle dashes if any
        .astype(float)
    )

    # Sort to get top 5 banks
    df = df.sort_values(by="Total_Assets_USD_Billion", ascending=False).head(5)
    
    log("Transformed data: Top 5 Banks by Assets")
    print(df)

    return df

def load_to_csv(df, filename):
    df.to_csv(filename, index=False)
    log(f"Data saved to CSV: {filename}")

def load_to_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log(f"Data loaded into SQLite DB: {db_name}, Table: {table_name}")

def main():
    log("Preliminaries complete. Initiating ETL process.")

    df_extracted = extract()
    log("Data extraction complete. Initiating Transformation process.")

    df_transformed = transform(df_extracted)
    log("Data transformation complete. Initiating Loading process.")

    load_to_csv(df_transformed, "top_5_banks.csv")
    load_to_db(df_transformed, "banks.db", "top_banks")

    log("ETL process complete.")

if __name__ == "__main__":
    main()
