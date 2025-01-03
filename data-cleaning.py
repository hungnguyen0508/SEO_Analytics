import pandas as pd
import numpy as np
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

def load_data(file_paths):
    """Load multiple CSV files and concatenate them."""
    return pd.concat([pd.read_csv(path, parse_dates=["Date"]) for path in file_paths])

def clean_title_name(df):
    """Synchronize Title and Name columns."""
    df.loc[df.Title.isnull(), "Title"] = df.loc[df.Title.isnull(), "Name"]
    df.loc[df.Name.isnull(), "Name"] = df.loc[df.Name.isnull(), "Title"]
    return df

def clean_url_fields(df):
    """Clean and standardize URL-related fields."""
    # Fill MA URL from ma_path
    mask = (df["MA URL"].isnull()) & (df.ma_path.notnull())
    df.loc[mask, "MA URL"] = "https://uniace.vn" + df.loc[mask, "ma_path"]
    
    # Fill from URL and Link columns
    df.loc[(df["MA URL"].isnull()) & (df.URL.notnull()), "MA URL"] = df.loc[(df["MA URL"].isnull()) & (df.URL.notnull()), "URL"]
    link_mask = (df["MA URL"].isnull()) & (df.Link.str.contains("https://uniace.vn", na=False))
    df.loc[link_mask, "MA URL"] = df.loc[link_mask, "Link"]
    
    # Update ma_path from MA URL
    mask = (df["ma_path"].isnull()) & (df["MA URL"].notnull())
    df.loc[mask, "ma_path"] = df.loc[mask, "MA URL"].str.replace("https://uniace.vn", "")
    
    # Fill remaining nulls
    df["MA URL"] = df["MA URL"].fillna("Ongoing Use")
    df.loc[df.ma_path.isna(), "ma_path"] = df.loc[df.ma_path.isna(), "MA URL"]
    df["MA Referrer"] = df["MA Referrer"].fillna("Live Access")
    
    return df

def fix_misplaced_data(df):
    """Fix misplaced data across columns."""
    # Fix MA URL with IP errors
    ip_mask = (df["MA URL"].str.contains("^\\d", na=False)) & (df.Date.str.contains(r"^\d{2}-\d{2}-\d{4}", na=False))
    df.loc[ip_mask, "MA URL"] = "https://uniace.vn/" + df.loc[ip_mask, "ma_path"]
    
    # Move Message ID to CUID
    vyt_mask = df.cuid == "/vyt/"
    df.loc[vyt_mask, ["cuid", "Message Id"]] = df.loc[vyt_mask, ["Message Id", np.nan]]
    
    # Fix date and IP address issues
    date_mask = df["IP Address"].str.contains(r"^\d{2}-\d{2}-\d{4}", na=False)
    df.loc[date_mask, "Date"] = df.loc[date_mask, "IP Address"]
    
    return df

def standardize_date(date_str):
    """Convert dates to standard format."""
    if pd.isna(date_str):
        return date_str
        
    formats = ["%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%d-%m %H:%M:%S"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    return ""

def prepare_for_database(df):
    """Prepare dataframe for database insertion."""
    df = df.copy(deep=True)
    
    # Remove unnecessary columns
    columns_to_drop = ['Template Id', 'List Id', 'Form Id', 'Campaign Id', 
                      'Campaign Name', 'Scenario Id', 'URL', 'Link', 'Tag']
    df.drop(columns_to_drop, axis=1, inplace=True)
    
    # Rename columns
    column_mapping = {
        "MA URL": "MA_URL",
        "MA Referrer": "MA_Referrer",
        "Message Id": "Message_Id",
        "IP Address": "IP_Address"
    }
    df.rename(columns=column_mapping, inplace=True)
    
    return df

def create_database_table(cursor):
    """Create SQL table for data storage."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS SEO_table(
        Email varchar(50),
        Type varchar(20),
        Name varchar(200),
        Title varchar(200),
        Ma_URL varchar(200),
        Ma_Referrer varchar(200),
        ma_path varchar(200),
        IP_Address varchar(30),
        cuid varchar(200),
        Date datetime,
        Message_ID varchar(200)
    )
    """
    cursor.execute(create_table_sql)

def main():
    # File paths
    file_paths = [
        "C:\\Users\\VICTUS\\Desktop\\Longdata\\btvn 2\\bài 5\\Project\\Project\\Raw_data\\Uniace_1.csv",
        "C:\\Users\\VICTUS\\Desktop\\Longdata\\btvn 2\\bài 5\\Project\\Project\\Raw_data\\Uniace_2.csv",
        "C:\\Users\\VICTUS\\Desktop\\Longdata\\btvn 2\\bài 5\\Project\\Project\\Raw_data\\Uniace_3.csv"
    ]
    
    # Load and process data
    df = load_data(file_paths)
    df = clean_title_name(df)
    df = clean_url_fields(df)
    df = fix_misplaced_data(df)
    df["Date"] = df.Date.apply(standardize_date)
    
    # Prepare for database
    df_final = prepare_for_database(df)
    
    # Database operations
    try:
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            password="",
            database="SEO"
        )
        
        with mydb.cursor() as cursor:
            create_database_table(cursor)
            mydb.commit()
        
        # Import to database
        engine = create_engine("mysql+pymysql://root:@localhost/SEO")
        df_final.to_sql('SEO_table', con=engine, if_exists='append', index=False)
        
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'mydb' in locals():
            mydb.close()

if __name__ == "__main__":
    main()
