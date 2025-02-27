from extractor_class import StandardBudgetProcessor, LeMasserieBudgetProcessor, EspacioLeonBudgetProcessor, InvoiceApprovalsProcessor
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import types
import os


# Adatbázis kapcsolat
DB_URL = "postgresql+psycopg2://postgres:Szabi_9407@localhost:5433/ITAM_data"
engine = create_engine(DB_URL)

# DataFrame lista létrehozása
all_data = []

# Standard budget fájlok
STANDARD_FILES = [
    ("EdithFund2024.xlsx", StandardBudgetProcessor),
    ("OneVictoreiBudget2024.xlsx", StandardBudgetProcessor),
    ("AIBudget2024.xlsx", StandardBudgetProcessor),
    ("XantiumBudget2024.xlsx", StandardBudgetProcessor),
    ("PromenadaMallBudget2024.xlsx", StandardBudgetProcessor),
    ("RemsingBudget2024.xlsx", StandardBudgetProcessor),
    ("TaifunBudget2024.xlsx", StandardBudgetProcessor),
    ("HotelOscarBudget2024.xlsx", StandardBudgetProcessor),
    ("PortaSienaBudget2024.xlsx", StandardBudgetProcessor),
    ("VilamarinaBudget2024.xlsx", StandardBudgetProcessor),
    ("BonaireBudget2024.xlsx", StandardBudgetProcessor),
    ("BaneasBudget2024.xlsx", StandardBudgetProcessor),
    ("DounbyCBudget2024.xlsx", StandardBudgetProcessor),
    ("DounbyDBudget2024.xlsx", StandardBudgetProcessor),
    ("DounbyEBudget2024.xlsx", StandardBudgetProcessor),
    ("DounbyFBudget2024.xlsx", StandardBudgetProcessor)
]

# Speciális budget fájlok
SPECIAL_FILES = [
    ("LeMasserieBudget2024.xlsx", LeMasserieBudgetProcessor),
    ("EspacioLeonBudget2024.xlsx", EspacioLeonBudgetProcessor)
]

INVOICE_APPROVAL_FILES = [
    "OneVictoreiBudget2024.xlsx",
    "AIBudget2024.xlsx",
    "XantiumBudget2024.xlsx",
    "PromenadaMallBudget2024.xlsx",
    "RemsingBudget2024.xlsx",
    "TaifunBudget2024.xlsx",
    "HotelOscarBudget2024.xlsx",
    "PortaSienaBudget2024.xlsx",
    "VilamarinaBudget2024.xlsx",
    "BonaireBudget2024.xlsx",
    "BaneasBudget2024.xlsx",
    "DounbyCBudget2024.xlsx",
    "DounbyDBudget2024.xlsx",
    "DounbyEBudget2024.xlsx",
    "DounbyFBudget2024.xlsx",
    "LeMasserieBudget2024.xlsx",
    "EspacioLeonBudget2024.xlsx"
]

# Fájlok feldolgozása (standard)
for file_name, ProcessorClass, *optional_tab in STANDARD_FILES:
    file_path = f"C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/BUDGETS/2024 Budgets/{file_name}"
    print(f"Processing file: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    processor = ProcessorClass(file_path, DB_URL)

    # Extract asset name a megfelelő tab-ról
    tab_name = optional_tab[0] if optional_tab else "2024 Budget vs Actual"
    try:
        processor.extract_asset_name(tab_name=tab_name)
        print(f"Asset name extracted from tab: {tab_name}")
    except Exception as e:
        print(f"Error extracting asset name from tab {tab_name}: {e}")
        continue

    # Extract monthly data
    try:
        monthly_data = processor.extract_monthly_data(tab_name=tab_name)
        print("Monthly data extracted successfully.")
    except Exception as e:
        print(f"Error extracting monthly data: {e}")
        continue

    # Cost Code módosítása
    try:
        processor.modify_cost_code()
        print("Cost Code modified successfully.")
    except Exception as e:
        print(f"Error modifying Cost Code: {e}")
        continue

    # Adatok összegyűjtése
    all_data.append(monthly_data)

# Speciális fájlok feldolgozása
for file_name, ProcessorClass, *optional_cell in SPECIAL_FILES:
    file_path = f"C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/BUDGETS/2024 Budgets/{file_name}"
    print(f"Processing special file: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    processor = ProcessorClass(file_path, DB_URL)

    # Asset név kinyerése a megfelelő cellából
    cell_ref = optional_cell[0] if optional_cell else "D4"
    try:
        processor.extract_asset_name(cell_ref=cell_ref)
        print(f"Asset name extracted from cell: {cell_ref}")
    except Exception as e:
        print(f"Error extracting asset name from cell {cell_ref}: {e}")
        continue

    # Havi adatok kinyerése
    try:
        monthly_data = processor.extract_monthly_data()
        print("Monthly data extracted successfully.")
    except Exception as e:
        print(f"Error extracting monthly data: {e}")
        continue

    # Cost Code módosítása
    try:
        processor.modify_cost_code()
        print("Cost Code modified successfully.")
    except Exception as e:
        print(f"Error modifying Cost Code: {e}")
        continue

    # Adatok hozzáadása a DataFrame listához
    all_data.append(monthly_data)

# Az összes adat egyesítése egy DataFrame-be
if not all_data:
    raise ValueError("No data processed. Check input files and processing steps.")

final_df = pd.concat(all_data, ignore_index=True)

# Numerikus adatok konvertálása float típusra
months_columns = [col for col in final_df.columns if col.startswith("2024-")]

for col in months_columns:
    try:
        final_df[col] = final_df[col].astype(str).str.replace(",", ".").astype(float, errors="ignore")
        print(f"Column {col} converted to float.")
    except Exception as e:
        print(f"Error converting column {col} to float: {e}")

# **Unpivotálás a havi oszlopokra**
try:
    unpivoted_df = final_df.melt(
        id_vars=["Cost Code", "Cost Name", "Asset"],  # Ezek az oszlopok maradnak változatlanul
        value_vars=months_columns,  # Ezek az oszlopok kerülnek unpivotálásra
        var_name="Hónap",  # Az új oszlop neve az eredeti oszlopnevek számára
        value_name="Érték"  # Az új oszlop neve az értékek számára
    )
    print("Unpivotálás sikeresen végrehajtva.")
except Exception as e:
    print(f"Error during unpivot: {e}")

# **Hónap oszlop konvertálása dátum formátumra**
try:
    unpivoted_df["Hónap"] = pd.to_datetime(unpivoted_df["Hónap"], format="%Y-%m")  # Hónap oszlop átalakítása
    print("Hónap oszlop konvertálva dátum formátumra.")
except Exception as e:
    print(f"Error converting 'Hónap' column to datetime: {e}")

# **Quarter oszlop hozzáadása**
try:
    unpivoted_df["Quarter"] = unpivoted_df["Hónap"].dt.quarter
    unpivoted_df["Quarter"] = "Q" + unpivoted_df["Quarter"].astype(str)  # Előtag hozzáadása
    print("Quarter oszlop sikeresen hozzáadva.")
except Exception as e:
    print(f"Error adding 'Quarter' column: {e}")


# Feltöltés az SQL adatbázisba
try:
    unpivoted_df.to_sql(
        "combined_budget_2024",
        con=engine.connect(),
        if_exists="replace",
        index=False,
        dtype={
            "Cost Code": types.VARCHAR(50),
            "Cost Name": types.VARCHAR(255),
            "Asset": types.VARCHAR(255),
            "Hónap": types.DATE,  # Hónap oszlop DATE adattípusként tárolva
            "Érték": types.FLOAT,  # Az érték oszlop numerikus formátum
            "Quarter": types.VARCHAR(10)
        }
    )
    print("Az unpivotált adatok sikeresen feltöltve az SQL adatbázisba!")
except Exception as e:
    print(f"Error uploading unpivoted data to SQL database: {e}")


# List to store all extracted invoice approvals data
all_invoice_data = []

# Process invoice approvals
for file_name in INVOICE_APPROVAL_FILES:
    file_path = f"C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/BUDGETS/2024 Budgets/{file_name}"
    print(f"Processing invoice approvals from: {file_path}")

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}, skipping.")
        continue

    invoice_processor = InvoiceApprovalsProcessor(file_path, DB_URL)

    try:
        extracted_data = invoice_processor.extract_invoice_data()  # Extract data
        print("Invoice approvals extracted successfully.")

        # Extract correct asset name using a StandardBudgetProcessor instance
        asset_name = StandardBudgetProcessor(file_path, DB_URL).extract_asset_name()
        extracted_data["Asset"] = asset_name

        # Append the extracted data to the list
        all_invoice_data.append(extracted_data)

    except Exception as e:
        print(f"Error extracting invoice approvals: {e}")
        continue

    # Ensure all extracted data is combined before uploading
    if all_invoice_data:
        final_invoice_df = pd.concat(all_invoice_data, ignore_index=True)

        # Upload to SQL
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                final_invoice_df.to_sql(
                    "invoice_approvals",
                    con=conn,
                    if_exists="replace",
                    index=False,
                    dtype={"Asset": types.VARCHAR(255)}
                )
            print("All invoice approvals data uploaded successfully.")
        except Exception as e:
            print(f"Error uploading invoice approvals data: {e}")
    else:
        print("No invoice approvals data processed. Skipping SQL upload.")

