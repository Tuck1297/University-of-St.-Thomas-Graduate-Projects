import cv2
import pytesseract
import numpy as np
import pdf2image
import csv
import os
from PIL import Image

# Configuration
pdf_path = "downloads/2025_MN_State_Parks_data.pdf"
output_csv = "project_data_export/mn_state_parks_data_ocr.csv"
temp_img_path = "temp_page_2.png"

def ocr_extract_table():
    if not os.path.exists("project_data_export"):
        os.makedirs("project_data_export")

    print(f"Converting PDF page 2 to image...")
    try:
        # Convert page 2 (index 1) to an image
        # Note: poppler must be installed for pdf2image to work
        images = pdf2image.convert_from_path(pdf_path, first_page=2, last_page=2)
        if not images:
            print("Failed to convert PDF page.")
            return
        
        img = images[0]
        img.save(temp_img_path)
        print(f"Page 2 saved as {temp_img_path}")
        
    except Exception as e:
        print(f"Error during PDF conversion: {e}")
        print("Tip: Make sure 'poppler' is installed and in your PATH.")
        return

    # Process image with OpenCV
    print("Processing image with OpenCV...")
    image = cv2.imread(temp_img_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # Thresholding to get a binary image
    thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

    # Detect horizontal and vertical lines to identify table structure
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40, 1))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 40))
    
    # Detect horizontal lines
    detect_horizontal = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    
    # Detect vertical lines
    detect_vertical = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    
    # Combine horizontal and vertical lines to find table grid
    table_grid = cv2.addWeighted(detect_horizontal, 0.5, detect_vertical, 0.5, 0.0)
    table_grid = cv2.threshold(table_grid, 0, 255, cv2.THRESH_BINARY)[1]

    # Find contours (cells) from the grid
    contours, _ = cv2.findContours(table_grid, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    
    print(f"Found {len(contours)} potential table cells/contours.")

    # Use Tesseract to get the text from the entire page
    print("Running Tesseract OCR on page (this may take a moment)...")
    try:
        # We'll use --psm 6 (Assume a single uniform block of text) or 
        # --psm 11 (Sparse text. Find as much text as possible in no particular order.)
        ocr_data = pytesseract.image_to_string(img, config='--psm 6')
        print("OCR Data (first 200 chars):")
        print(ocr_data[:200])
        
        # Note: Structuring OCR output into a precise table automatically is complex.
        # This script provides the raw OCR output for now as a starting point.
        with open(output_csv, "w", encoding="utf-8") as f:
            f.write(ocr_data)
            
        print(f"OCR output saved to {output_csv}")
        
    except Exception as e:
        print(f"Error during OCR: {e}")
        print("Tip: Make sure 'tesseract' is installed and in your PATH.")

    # Cleanup
    if os.path.exists(temp_img_path):
        os.remove(temp_img_path)

if __name__ == "__main__":
    ocr_extract_table()
