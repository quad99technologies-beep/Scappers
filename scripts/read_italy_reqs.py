
import pandas as pd
import sys

def read_excel_content(file_path):
    try:
        # Read all sheets
        xls = pd.ExcelFile(file_path)
        
        print(f"File: {file_path}")
        print(f"Sheets: {xls.sheet_names}")
        print("-" * 50)
        
        for sheet_name in xls.sheet_names:
            print(f"\nSheet: {sheet_name}")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Print column names
            print(f"Columns: {list(df.columns)}")
            
            # Print first few rows to get an idea of data
            print("First 5 rows:")
            print(df.head().to_string())
            
            # Print any specific instructions or text if it looks like a document
            # Sometimes requirements are in a specific cell or column
            # We'll just dump the non-null content of the first few columns
            print("\nContent overview:")
            print(df.to_string())
            print("-" * 50)
            
    except Exception as e:
        print(f"Error reading Excel file: {e}")

if __name__ == "__main__":
    file_path = r"C:\Users\Vishw\Downloads\Italy Aifa_Updated_Scrapping Doc_20263001_v1.xlsx"
    read_excel_content(file_path)
