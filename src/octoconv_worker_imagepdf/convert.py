"""One PNG in, one single-page PDF out."""

import img2pdf


def png_to_pdf(png: bytes) -> bytes:
    if not png.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("png")
    pdf = img2pdf.convert(png)
    if not isinstance(pdf, bytes) or not pdf.startswith(b"%PDF-"):
        raise ValueError("pdf")
    return pdf
