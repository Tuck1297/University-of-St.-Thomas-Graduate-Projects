from db_utils import handle_existing_data

def run(conn, existed):
    if existed:
        if not handle_existing_data(conn, "mn_dnr.data", "Minnesota DNR", schema_name="mn_dnr"):
            return

    print("Running Minnesota DNR job")
    # TODO: Implement Minnesota DNR ingestion logic

    # PDF Image Extraction Notes (Future Implementation)
    """
    NOTES FOR PDF IMAGE EXTRACTION (for inclusion in mn_gis ingestion pipeline):
    
    If future datasets include PDFs (e.g., park maps or brochures), the following approach 
    could be used to extract images and text:
    
    1. Libraries to consider:
       - PyMuPDF (fitz): Excellent for extracting images directly from PDF pages without rasterizing.
       - pdf2image: Useful for converting entire pages to images (rasterizing).
       - Pillow (PIL): For image processing and saving extracted assets.
       - pytesseract: If OCR is required for text within images.
    
    2. Implementation Strategy:
       import fitz # PyMuPDF
       
       def extract_images_from_pdf(pdf_path, output_dir):
           pdf_file = fitz.open(pdf_path)
           for page_index in range(len(pdf_file)):
               page = pdf_file[page_index]
               image_list = page.get_images(full=True)
               for img_index, img in enumerate(image_list):
                   xref = img[0]
                   base_image = pdf_file.extract_image(xref)
                   image_bytes = base_image["image"]
                   image_ext = base_image["ext"]
                   image_name = f"page{page_index+1}_img{img_index+1}.{image_ext}"
                   with open(os.path.join(output_dir, image_name), "wb") as f:
                       f.write(image_bytes)
    
    3. Integration with DuckDB:
       - Store image metadata (path, page number, original PDF name) in a dedicated table.
       - If OCR is used, store the extracted text in a searchable text column.
    """

