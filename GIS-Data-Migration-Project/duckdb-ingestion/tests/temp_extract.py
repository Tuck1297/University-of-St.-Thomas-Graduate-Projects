import pdfplumber
import csv
import os

pdf_path = "downloads/2025_MN_State_Parks_data.pdf"
output_csv = "project_data_export/mn_state_parks_data.csv"

def flip_text(text):
    if not text:
        return text
    return "\n".join(["".join(reversed(line)) for line in text.split("\n")])

def extract_table():
    if not os.path.exists("project_data_export"):
        os.makedirs("project_data_export")

    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) < 2:
            print("PDF has less than 2 pages.")
            return
        print(f"Total pages in PDF: {len(pdf.pages)}")
        second_page = pdf.pages[1] # Second page
        
        print(f"Extracting table from page 2 (this may take a minute due to complexity)...")
        table = second_page.extract_table({
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
        })
        
        if table:
            print(f"Extracted {len(table)} rows. Saving to {output_csv}...")
            cleaned_table = []
            for i, row in enumerate(table):
                # Clean up the row. Only the header row (index 0) seems to be reversed.
                if i == 0:
                    cleaned_row = [flip_text(cell) for cell in row]
                else:
                    # Replace newlines with spaces for CSV compatibility
                    cleaned_row = [cell.replace("\n", " ") if cell else cell for cell in row]
                cleaned_table.append(cleaned_row)
            
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(cleaned_table)
            
            print("Done.")
        else:
            print("No table found on the second page.")

if __name__ == "__main__":
    extract_table()
