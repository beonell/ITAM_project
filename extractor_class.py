from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy.types as types


class StandardBudgetProcessor:
    def __init__(self, file_path, db_url):
        self.file_path = file_path
        self.db_url = db_url
        self.asset_name = None
        self.data = None

    def extract_asset_name(self, cell_ref="D4", tab_name="2024 Budget vs Actual"):
        """Extract the asset name from a specific cell."""
        xl = pd.ExcelFile(self.file_path)
        sheet = xl.parse(tab_name, header=None)
        col, row = self._convert_cell_ref(cell_ref)
        self.asset_name = sheet.iloc[row, col]  # Extract asset name based on cell_ref
        return self.asset_name

    def extract_monthly_data(self, tab_name="2024 Budget vs Actual"):
        """Extract the monthly data (D:O)."""
        xl = pd.ExcelFile(self.file_path)
        sheet = xl.parse(tab_name, header=None)

        # Identify the first cost-ID row (typically row 22 in Excel, Python index 21)
        start_row = 21
        data = sheet.iloc[start_row:, 3:15]  # Columns D:O

        # Normalize the table (Cost Codes + Cost Names + Monthly Data)
        month_names = pd.date_range(start="2024-01", periods=12, freq="MS").strftime("%Y-%m").tolist()
        data.columns = month_names
        data.insert(0, "Cost Code", sheet.iloc[start_row:, 0])  # Column A (Code)
        data.insert(1, "Cost Name", sheet.iloc[start_row:, 2])  # Column C (Name)

        # Új oszlop beszúrása az asset névvel
        self.extract_asset_name(tab_name=tab_name)  # Asset név kinyerése
        data["Asset"] = self.asset_name  # Új oszlop létrehozása minden sorban ugyanazzal az értékkel

        self.data = data.dropna(subset=["Cost Code"])  # Üres "Cost Code" sorok szűrése
        return self.data

    def modify_cost_code(self):
        """Handle 'Actual' text and duplicates in Cost Code."""
        # Ensure 'Cost Code' and 'Cost Name' columns are strings
        self.data["Cost Code"] = self.data["Cost Code"].astype(str)
        self.data["Cost Name"] = self.data["Cost Name"].fillna("").astype(str)

        # If "Actual" is in Cost Name, adjust Cost Code
        self.data["Cost Code"] = self.data.apply(
            lambda row: f"{row['Cost Code']}-A" if "Actual" in row["Cost Name"] else row["Cost Code"], axis=1
        )

        # Handle duplicates
        duplicates = self.data.duplicated(subset=["Cost Code", "Cost Name"], keep="first")
        self.data.loc[duplicates, "Cost Code"] = self.data.loc[duplicates, "Cost Code"] + "-A"

    def upload_to_sql(self, table_name):
        """
        Upload the extracted data to an SQL database with explicit column types.
        """
        # Ensure correct numeric formatting
        for col in self.data.columns:
            if self.data[col].dtype == "object":
                self.data[col] = self.data[col].apply(
                    lambda x: str(x).replace(",", ".") if pd.notnull(x) else x
                ).astype(float, errors="ignore")

        # Define SQL column types
        column_types = {
            "Cost Code": types.VARCHAR(50),
            "Cost Name": types.VARCHAR(255),
            "Asset": types.VARCHAR(255),  # New column SQL type
            "2024-01": types.FLOAT,
            "2024-02": types.FLOAT,
            "2024-03": types.FLOAT,
            "2024-04": types.FLOAT,
            "2024-05": types.FLOAT,
            "2024-06": types.FLOAT,
            "2024-07": types.FLOAT,
            "2024-08": types.FLOAT,
            "2024-09": types.FLOAT,
            "2024-10": types.FLOAT,
            "2024-11": types.FLOAT,
            "2024-12": types.FLOAT
        }

        # Create database connection and upload
        engine = create_engine(self.db_url)
        with engine.connect() as conn:
            self.data.to_sql(
                table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype=column_types
            )
        print(f"Data uploaded to table: {table_name}")

    def _convert_cell_ref(self, cell_ref):
        col = ord(cell_ref[0].upper()) - ord('A')
        row = int(cell_ref[1:]) - 1
        return col, row


class LeMasserieBudgetProcessor(StandardBudgetProcessor):
    def extract_monthly_data(self, tab_name="2024 Budget vs Actual"):
        """Handle specific rows for LeMasserie."""
        return super().extract_monthly_data(tab_name=tab_name)


class EspacioLeonBudgetProcessor(StandardBudgetProcessor):
    def extract_asset_name(self, cell_ref="D4", tab_name="2024 Budget vs Actual"):
        return super().extract_asset_name(cell_ref=cell_ref, tab_name=tab_name)

    def extract_monthly_data(self, tab_name="2024 Budget vs Actual"):
        """Handle specific structure for Espacio Leon."""
        return super().extract_monthly_data(tab_name=tab_name)


class DounbyBudgetProcessor(StandardBudgetProcessor):
    def extract_asset_name(self, cell_ref="C4", tab_name="WBP 4 ALL"):
        return super().extract_asset_name(cell_ref=cell_ref, tab_name=tab_name)

    def extract_monthly_data(self, tab_name="WBP 4 ALL"):
        return super().extract_monthly_data(tab_name=tab_name)