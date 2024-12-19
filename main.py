from extractor_class import StandardBudgetProcessor, LeMasserieBudgetProcessor, EspacioLeonBudgetProcessor, \
    DounbyBudgetProcessor
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import types
import logging

# Alapértelmezett logbeállítás
logging.basicConfig(
    filename=r'C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/script_log.txt',  # Teljes elérési út
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Script started")

try:
    # Adatbázis kapcsolat
    DB_URL = "postgresql+psycopg2://postgres:Szabi_9407@localhost:5433/ITAM_data"
    engine = create_engine(DB_URL)

    # DataFrame lista létrehozása
    all_data = []

    # Standard budget fájlok
    STANDARD_FILES = [
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
    ]

    # Speciális budget fájlok
    SPECIAL_FILES = [
        ("LeMasserieBudget2024.xlsx", LeMasserieBudgetProcessor),
        ("EspacioLeonBudget2024.xlsx", EspacioLeonBudgetProcessor, "C4"),
        ("DounbyBudget2024.xlsx", DounbyBudgetProcessor)
    ]

    # Fájlok feldolgozása (standard)
    for file_name, ProcessorClass, *optional_tab in STANDARD_FILES:
        file_path = f"C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/BUDGETS/2024 Budgets/{file_name}"
        processor = ProcessorClass(file_path, DB_URL)

        # Extract asset name a megfelelő tab-ról
        tab_name = optional_tab[0] if optional_tab else "2024 Budget vs Actual"
        processor.extract_asset_name(tab_name=tab_name)

        # Extract monthly data
        monthly_data = processor.extract_monthly_data(tab_name=tab_name)

        # Cost Code módosítása
        processor.modify_cost_code()

        # Adatok összegyűjtése
        all_data.append(monthly_data)

    # Speciális fájlok feldolgozása
    for file_name, ProcessorClass, *optional_cell in SPECIAL_FILES:
        file_path = f"C:/Users/vass.szabolcs/OneDrive - Indotek Zrt/Asztal/projects/ITAM/BUDGETS/2024 Budgets/{file_name}"
        processor = ProcessorClass(file_path, DB_URL)

        # Asset név kinyerése a megfelelő cellából
        cell_ref = optional_cell[0] if optional_cell else "D4"
        processor.extract_asset_name(cell_ref=cell_ref)

        # Havi adatok kinyerése
        monthly_data = processor.extract_monthly_data()

        # Cost Code módosítása
        processor.modify_cost_code()

        # Adatok hozzáadása a DataFrame listához
        all_data.append(monthly_data)

    # Az összes adat egyesítése egy DataFrame-be
    final_df = pd.concat(all_data, ignore_index=True)

    # Numerikus adatok konvertálása float típusra
    months_columns = [col for col in final_df.columns if col.startswith("2024-")]

    for col in months_columns:
        final_df[col] = final_df[col].astype(str).str.replace(",", ".").astype(float, errors="ignore")

    # Feltöltés az SQL adatbázisba
    final_df.to_sql(
        "combined_budget_2024",
        con=engine.connect(),
        if_exists="replace",
        index=False,
        dtype={
            "Cost Code": types.VARCHAR(50),
            "Cost Name": types.VARCHAR(255),
            "Asset": types.VARCHAR(255),
            **{month: types.FLOAT for month in months_columns},
        }
    )

    print("Az összesített adatok sikeresen feltöltve az SQL adatbázisba!")

except Exception as e:
    logging.error(f"Error occurred: {e}")