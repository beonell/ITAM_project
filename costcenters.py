import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine
import sqlalchemy.types as types

# Define the directory containing the budget files
budget_files_dir = 'C:\\Users\\vass.szabolcs\\OneDrive - Indotek Zrt\\Asztal\\projects\\ITAM\\BUDGETS\\2024 Budgets'

# Initialize an empty DataFrame for the cost centers
costcenters_df = pd.DataFrame(columns=['CostCode', 'CostName'])

# Parent-Child relationship (based on earlier discussions)
parent_child_hierarchy = {
    "10430": ["10400", "10432", "10408", "10404", "10402", "10433", "10401"],
    "21000": ["10420", "10405", "10450"],
    "21010": ["21011", "21021", "21026", "21031", "21041", "21022"],
    "21046": ["21246", "21248"],
    "21120": ["21101", "21102", "21103", "21104", "21105", "21106", "21107", "21221", "CW00007", "21108", "21111"],
    "21121": ["21051", "21061", "21071", "21081", "21110", "21170", "21220"],
    "21219": ["21091", "21190", "21200"],
    "21050": ["21010", "21046", "21120", "21121", "21219"],
    "22200": ["22101", "21100", "10430"],
    "22210": ["22200", "21230", "21240", "21180", "21250", "21260", "21270", "21280", "21290", "21320", "21360",
              "CW0004", "CW0005", "21362", "21400", "21366", "24046", "24030", "24010", "24050"],
    "30040": ["30040LC", "30040CLC", "30012", "30030"],
    "30090": ["30051", "30052", "30053", "30054", "30055", "30056", "30057", "30058"],
    "23400": ["22210", "30040", "30090"],
    "27090": ["24040", "24042", "24045", "27010", "24048"],
    "35999": ["26000", "26003", "31010", "26010", "31020"],
    "39999": ["39030"],
    "10430-A": ["10400-A", "10432-A", "10408-A", "10404-A", "10402-A", "10433-A", "10401-A"],
    "21000-A": ["10420-A", "10405-A", "10450-A"],
    "21010-A": ["21011-A", "21021-A", "21026-A", "21031-A", "21041-A", "21022-A"],
    "21046-A": ["21246-A", "21248-A"],
    "21120-A": ["21101-A", "21102-A", "21103-A", "21104-A", "21105-A", "21106-A", "21107-A", "21221-A",
                "CW00007-A", "21108-A", "21111-A"],
    "21121-A": ["21051-A", "21061-A", "21071-A", "21081-A", "21110-A", "21170-A", "21220-A"],
    "21219-A": ["21091-A", "21190-A", "21200-A"],
    "21050-A": ["21010-A", "21046-A", "21120-A", "21121-A", "21219-A"],
    "22200-A": ["22101-A", "21100-A", "10430-A"],
    "22210-A": ["22200-A", "21230-A", "21240-A", "21180-A", "21250-A", "21260-A", "21270-A", "21280-A",
                "21290-A", "21320-A", "21360-A", "CW0004-A", "CW0005-A", "21362-A", "21400-A", "21366-A",
                "24046-A", "24030-A", "24010-A", "24050-A"],
    "30040-A": ["30040LC-A", "30040CLC-A", "30012-A", "30030-A"],
    "30090-A": ["30051-A", "30052-A", "30053-A", "30054-A", "30055-A", "30056-A", "30057-A", "30058-A"],
    "23400-A": ["22210-A", "30040-A", "30090-A"],
    "27090-A": ["24040-A", "24042-A", "24045-A", "27010-A", "24048-A"],
    "35999-A": ["26000-A", "26003-A", "31010-A", "26010-A", "31020-A"],
    "39999-A": ["39030-A"]
}

# Reverse the hierarchy for faster lookup
child_to_parent = {child: parent for parent, children in parent_child_hierarchy.items() for child in children}

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

# Add the Parent Cost Code column to the DataFrame
costcenters_df['Parent Cost Code'] = costcenters_df['CostCode'].apply(lambda x: child_to_parent.get(x, None))

# Add the Is Aggregate column to the DataFrame
costcenters_df['Is Aggregate'] = costcenters_df['CostCode'].isin(parent_child_hierarchy.keys())


# Add the Cleaned Cost Code column
costcenters_df['Cleaned Cost Code'] = costcenters_df['CostCode'].apply(
    lambda x: x[:-2] if x.endswith('-A') else x
)

# Add the Cleaned Cost Name column
costcenters_df['Cleaned Cost Name'] = costcenters_df['CostName'].apply(
    lambda x: x.replace(" - Actual", "") if " - Actual" in x else x
)

# Add the Combined Column
costcenters_df['Combined Column'] = costcenters_df['Cleaned Cost Code'] + ' - ' + costcenters_df['Cleaned Cost Name']

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
                'CostName': types.VARCHAR(255),
                'Parent Cost Code': types.VARCHAR(50),
                'Is Aggregate': types.BOOLEAN,
                'Cleaned Cost Code': types.VARCHAR(50),
                'Combined Column': types.VARCHAR(255)
            }
        )
        print("Cost centers table with Parent Cost Code and Is Aggregate uploaded successfully to ITAM_data database.")
    except Exception as e:
        print(f"Error uploading to SQL database: {e}")