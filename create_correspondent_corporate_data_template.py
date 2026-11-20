from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation


OUTPUT = Path(__file__).with_name("CORRESPONDENT_BANK_PERSONAL_DATA_TEMPLATE.xlsx")

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
TITLE_FILL = PatternFill("solid", fgColor="D9EAF7")
WHITE_BOLD = Font(color="FFFFFF", bold=True)
BOLD = Font(bold=True)


def style_header(ws, row):
    for cell in ws[row]:
        cell.fill = HEADER_FILL
        cell.font = WHITE_BOLD
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.row_dimensions[row].height = 32
    ws.freeze_panes = f"A{row + 1}"
    ws.auto_filter.ref = f"A{row}:{ws.cell(ws.max_row, ws.max_column).coordinate}"


def fit_columns(ws):
    for column in ws.columns:
        letter = column[0].column_letter
        width = max(len(str(cell.value or "")) for cell in column) + 2
        ws.column_dimensions[letter].width = min(max(width, 14), 42)


book = Workbook()
readme = book.active
readme.title = "Read me"

readme["A1"] = "Correspondent-bank PersonalDataCorporates data collection template"
readme["A1"].font = Font(bold=True, size=14)
readme["A1"].fill = TITLE_FILL
readme.merge_cells("A1:D1")
readme["A3"] = "Purpose"
readme["B3"] = "Collect the source values absent or invalid for the two correspondent-bank customers before they are added to personalDataCorporates."
readme["A4"] = "Complete"
readme["B4"] = "Fill blank cells in the Corporate profiles and Related persons sheets. Do not change the prefilled identifiers or reference values."
readme["A5"] = "Dates"
readme["B5"] = "Use YYYY-MM-DD for establishedDate and appointmentDate."
readme["A6"] = "Related persons"
readme["B6"] = "Add one row for every required director, authorised person, shareholder, or other relevant related person."
readme["A7"] = "Known source values"
readme["B7"] = "Company names, account numbers, source addresses, and the Citibank region are prefilled only as reference."
for cell in ("A3", "A4", "A5", "A6", "A7"):
    readme[cell].font = BOLD
for row in readme.iter_rows(min_row=3, max_row=7, min_col=1, max_col=2):
    for cell in row:
        cell.alignment = Alignment(vertical="top", wrap_text=True)
readme.column_dimensions["A"].width = 24
readme.column_dimensions["B"].width = 105

profiles = book.create_sheet("Corporate profiles")
profile_headers = [
    "customerId", "institution", "accountNumber", "establishedDate", "numberOfEmployees",
    "totalEmployeesMale", "totalEmployeesFemale", "registrationNumber",
    "taxIdentificationNumber", "certificateIncorporation", "zipCode", "region",
    "sourceAddress", "sourceReferenceDate", "internalSourceOrNotes",
]
profiles.append(profile_headers)
profiles.append([
    "9999901", "BANK OF TANZANIA", "9999010002", "", "", "", "", "", "", "", "", "",
    "MIRAMBO ST, DAR ES SALAAM", "2016-03-24", "",
])
profiles.append([
    "9999902", "CITIBANK NEW YORK", "9999020001", "", "", "", "", "", "", "", "", "NEW YORK",
    "NEW YORK, NEW YORK", "2016-03-24", "",
])
style_header(profiles, 1)
for row in profiles.iter_rows(min_row=2, max_row=3, min_col=4, max_col=12):
    for cell in row:
        if cell.value == "":
            cell.fill = PatternFill("solid", fgColor="FFF2CC")
for row in range(2, 4):
    profiles.cell(row, 4).number_format = "yyyy-mm-dd"
fit_columns(profiles)

related = book.create_sheet("Related persons")
related_headers = [
    "customerId", "institution", "accountNumber", "fullName", "gender", "cellPhone",
    "relationType", "nationalId", "nationality", "appointmentDate", "internalSourceOrNotes",
]
related.append(related_headers)
for customer_id, institution, account in [
    ("9999901", "BANK OF TANZANIA", "9999010002"),
    ("9999902", "CITIBANK NEW YORK", "9999020001"),
]:
    for _ in range(5):
        related.append([customer_id, institution, account, "", "", "", "", "", "", "", ""])
style_header(related, 1)
for row in related.iter_rows(min_row=2, max_row=11, min_col=4, max_col=11):
    for cell in row:
        cell.fill = PatternFill("solid", fgColor="FFF2CC")
for row in range(2, 12):
    related.cell(row, 10).number_format = "yyyy-mm-dd"
gender_validation = DataValidation(type="list", formula1='"Male,Female"', allow_blank=True)
related.add_data_validation(gender_validation)
gender_validation.add("E2:E100")
fit_columns(related)

for sheet in (profiles, related):
    for row in sheet.iter_rows():
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

book.save(OUTPUT)
print(OUTPUT)
