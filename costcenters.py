import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine
import sqlalchemy.types as types

# Define the directory containing the budget files
budget_files_dir = 'C:\\Users\\vass.szabolcs\\OneDrive - Indotek Zrt\\Asztal\\projects\\ITAM\\BUDGETS\\2024 Budgets'

# Initialize an empty DataFrame for the cost centers
costcenters_df = pd.DataFrame(columns=['CostCode', 'CostName'])

# Loop through each budget file
for file_name in os.listdir(budget_files_dir):
    if file_name.endswith('.xlsx'):
        file_path = os.path.join(budget_files_dir, file_name)
        try:
            # Read only the '2024 Budget vs Actual' sheet
            sheet_df = pd.read_excel(file_path, sheet_name='2024 Budget vs Actual', usecols="A,C,B", header=None,
                                     dtype=str)

            if sheet_df.shape[1] >= 2:  # Ensure the sheet has at least two columns
                # Check for CostName in C or fallback to B if C is numeric
                sheet_df.columns = ['CostCode', 'FallbackCostName', 'CostName'][:sheet_df.shape[1]]
                sheet_df['CostName'] = sheet_df.apply(
                    lambda row: row['FallbackCostName']
                    if pd.notna(row['CostName']) and row['CostName'].replace('.', '', 1).isdigit()
                    else row['CostName'],
                    axis=1
                )

                extracted_df = sheet_df[['CostCode', 'CostName']].dropna(subset=['CostCode']).copy()

                # Ensure CostCode is treated as a clean string and trimmed to 50 characters
                extracted_df['CostCode'] = extracted_df['CostCode'].apply(
                    lambda x: str(int(float(x))) if pd.notna(x) and x.replace('.', '', 1).isdigit() else str(x)
                ).str[:50]

                # Ensure CostName is also a string and trimmed to 255 characters
                extracted_df['CostName'] = extracted_df['CostName'].fillna('').astype(str).str[:255]

                # Add the original extracted data to the main DataFrame
                costcenters_df = pd.concat([costcenters_df, extracted_df], ignore_index=True)

                # Create Actual pairs with '-A' suffix and modified CostName
                actual_df = extracted_df.copy()
                actual_df['CostCode'] = actual_df['CostCode'] + '-A'
                actual_df['CostName'] = actual_df['CostName'] + ' - Actual'

                # Add Actual pairs to the main DataFrame
                costcenters_df = pd.concat([costcenters_df, actual_df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")

# Drop duplicates based on CostCode and sort the cost centers
costcenters_df.drop_duplicates(subset=['CostCode'], inplace=True)
costcenters_df = costcenters_df.sort_values(by='CostCode', key=lambda x: x.astype(str))

# Check if DataFrame is empty
if costcenters_df.empty:
    print("No data extracted. Please check the input files and column names.")
else:
    # Define the SQL database connection
    db_url = "postgresql+psycopg2://postgres:Szabi_9407@localhost:5433/ITAM_data"
    engine = create_engine(db_url)

    # Upload the DataFrame to the SQL database
    try:
        costcenters_df.to_sql(
            'costcenters',
            engine,
            if_exists='replace',
            index=False,
            dtype={
                'CostCode': types.VARCHAR(50),
                'CostName': types.VARCHAR(255)
            }
        )
        print("Cost centers table uploaded successfully to ITAM_data database.")
    except Exception as e:
        print(f"Error uploading to SQL database: {e}")