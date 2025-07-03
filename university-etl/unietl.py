import requests
import pandas as pd 
from sqlalchemy import create_engine

def extract() -> dict:
    """This API extracts data from HTTP"""
    api_url = "http://universities.hipolabs.com/search?country=turkey"

    data = requests.get(api_url).json()
    return data

def transform(data: dict) -> pd.DataFrame:
    """Transform dataset into desired structure and filter"""
    df = pd.DataFrame(data)
    print(f"Total number of universities from API: {len(data)}")
    
    # Filter: names that contain "turkey" (case-insensitive)
    # df = df[df["name"].str.lower().str.contains("turkey")]
    print(f"Number of universities with 'turkey' in the name: {len(df)}")
    
    # Format lists to comma-separated strings
    df['domains'] = df['domains'].apply(lambda x: ','.join(map(str, x)))
    df['web_pages'] = df['web_pages'].apply(lambda x: ','.join(map(str, x)))
    
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]



def load(df:pd.DataFrame)->None:
    """load data into a sqlite database"""
    disk_engine = create_engine('sqlite:///store.db')

    df.to_sql('cal_uni',disk_engine, if_exists='replace')



data = extract()
df = transform(data)
load(df)




# Example usage
if __name__ == "__main__":
    raw_data = extract()
    clean_df = transform(raw_data)
    print(clean_df.head())


