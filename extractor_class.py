from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy.types as types
import openpyxl

class StandardBudgetProcessor:
    def __init__(self, file_path, db_url):
        self.file_path = file_path
        self.db_url = db_url
        self.data = None

    def extract_asset_name(self, cell_ref="D4", tab_name="2024 Budget vs Actual"):
        """Extract the asset name from a specific cell."""
        xl = pd.ExcelFile(self.file_path)
        sheet = xl.parse(tab_name, header=None)
        col, row = self._convert_cell_ref(cell_ref)
        return sheet.iloc[row, col]  # Extract asset name based on cell_ref

    def extract_monthly_data(self, tab_name="2024 Budget vs Actual"):
        """Extract the monthly data (D:O)."""
        xl = pd.ExcelFile(self.file_path)
        sheet = xl.parse(tab_name, header=None)

        start_row = 21
        data = sheet.iloc[start_row:, 3:15]  # Columns D:O

        month_names = pd.date_range(start="2024-01", periods=12, freq="MS").strftime("%Y-%m").tolist()
        data.columns = month_names
        data.insert(0, "Cost Code", sheet.iloc[start_row:, 0])  # Column A (Code)
        data.insert(1, "Cost Name", sheet.iloc[start_row:, 2])  # Column C (Name)

        asset_name = self.extract_asset_name()
        data["Asset"] = asset_name

        self.data = data.dropna(subset=["Cost Code"])
        return self.data

    def modify_cost_code(self):
        """Handle 'Actual' text and duplicates in Cost Code."""
        self.data["Cost Code"] = self.data["Cost Code"].astype(str)
        self.data["Cost Name"] = self.data["Cost Name"].fillna("").astype(str)

        self.data["Cost Code"] = self.data.apply(
            lambda row: f"{row['Cost Code']}-A" if "Actual" in row["Cost Name"] else row["Cost Code"], axis=1
        )

        duplicates = self.data.duplicated(subset=["Cost Code", "Cost Name"], keep="first")
        self.data.loc[duplicates, "Cost Code"] = self.data.loc[duplicates, "Cost Code"] + "-A"

    def upload_to_sql(self, table_name):
        """Upload the extracted data to an SQL database."""
        for col in self.data.columns:
            if self.data[col].dtype == "object":
                self.data[col] = self.data[col].apply(
                    lambda x: str(x).replace(",", ".") if pd.notnull(x) else x
                ).astype(float, errors="ignore")

        column_types = {
            "Cost Code": types.VARCHAR(50),
            "Cost Name": types.VARCHAR(255),
            "Asset": types.VARCHAR(255),
            **{month: types.FLOAT for month in pd.date_range(start="2024-01", periods=12, freq="MS").strftime("%Y-%m")}
        }

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
        return super().extract_monthly_data(tab_name=tab_name)

class EspacioLeonBudgetProcessor(StandardBudgetProcessor):
    def extract_asset_name(self, cell_ref="D4", tab_name="2024 Budget vs Actual"):
        return super().extract_asset_name(cell_ref=cell_ref, tab_name=tab_name)

    def extract_monthly_data(self, tab_name="2024 Budget vs Actual"):
        return super().extract_monthly_data(tab_name=tab_name)

class InvoiceApprovalsProcessor:
    def __init__(self, file_path, db_url):
        self.file_path = file_path
        self.db_url = db_url
        self.data = None

    def extract_invoice_data(self, tab_name="2024 Invoice Approvals"):
        xl = pd.ExcelFile(self.file_path)
        sheet = xl.parse(tab_name, header=None)

        row_offset = 3
        if "OneVictorei" in self.file_path:
            row_offset = 4

        headers = sheet.iloc[row_offset, :].astype(str).str.strip().tolist()

        required_col_names = {
            "Jira Number (Purchase Order)": "Jira Number (Purchase Order)",
            "Vendor (Contractor)": "Vendor (Contractor)",
            "Description": "Description",
            "Budget Line": "Budget Line",
            "Due Date": "Due Date",
            "Invoice Net Amount (Euro)": "Invoice Net Amount (Euro)",
            "Invoice Gross Amount (Euro)": "Invoice Gross Amount (Euro)",
            "Country Office Approval (Date)": "Country Office Approval (Date)",
            "HQ Approval (Date)": "HQ Approval (Date)",
            "Invoice Payment Date": "Invoice Payment Date"
        }

        # Handle Espacio Leon's specific column names
        if "EspacioLeon" in self.file_path:
            column_mapping = {
                "PO Number       (Purchase Order)": "Jira Number (Purchase Order)",
                "Vendor": "Vendor (Contractor)"
            }

            # Update headers dynamically
            headers = [column_mapping.get(col, col) for col in headers]

            # Get column indices **after mapping**
        col_indices = {col: headers.index(col) for col in required_col_names if col in headers}

        if len(col_indices) != len(required_col_names):
            missing_cols = set(required_col_names.keys()) - set(col_indices.keys())
            raise ValueError(f"Missing expected columns in {self.file_path}: {missing_cols}")

        data = sheet.iloc[row_offset + 1:, list(col_indices.values())]
        data.columns = [required_col_names[col] for col in col_indices.keys()]

        asset_name = StandardBudgetProcessor(self.file_path, self.db_url).extract_asset_name()
        data["Asset"] = asset_name  # Store correct asset per file

        self.data = data.dropna(how='all', subset=[col for col in data.columns if col != 'Asset'])
        # Remove aggregate rows where column A (first column) is populated
        self.data = self.data[self.data.iloc[:, 0].isna() == False]
        return self.data

    def upload_invoice_data_to_sql(self, table_name="invoice_approvals"):
        if self.data is None:
            raise ValueError("No data to upload. Run extract_invoice_data() first.")

        column_types = {"Asset": types.VARCHAR(255)}
        for col in self.data.columns:
            max_len = self.data[col].astype(str).str.len().max()
            col_size = min(max(255, max_len), 1000)
            column_types[col] = types.VARCHAR(col_size)

        engine = create_engine(self.db_url)
        with engine.connect() as conn:
            self.data.to_sql(
                table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype=column_types
            )

        print(f"Invoice approvals data uploaded to table: {table_name}")
