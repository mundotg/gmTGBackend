import pdfkit

path_wkhtmltopdf = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)

html_path = r"C:\Users\IT\Documents\portifolio\cv-ngueve.html"
css_path = r"C:\Users\IT\Documents\portifolio\style.css"  # Ajuste o nome se for outro
output_pdf = r"C:\Users\IT\Documents\portifolio\cv-ngueve-ngueve.pdf"

options = {
    "enable-local-file-access": "",  # importante!
    "page-size": "A4",
    "encoding": "UTF-8"
}

pdfkit.from_file(html_path, output_pdf, configuration=config)

print("PDF gerado com sucesso!")

# 