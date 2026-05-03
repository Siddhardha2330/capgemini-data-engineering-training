from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "FMCG_Distribution_Analytics_Project_Report.md"
OUTPUT = ROOT / "FMCG_Distribution_Analytics_Project_Report.pdf"


def parse_markdown_lines(text: str):
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            yield ("blank", "")
        elif line.startswith("# "):
            yield ("h1", line[2:].strip())
        elif line.startswith("## "):
            yield ("h2", line[3:].strip())
        elif line.startswith("### "):
            yield ("h3", line[4:].strip())
        elif line.startswith("- "):
            yield ("bullet", line[2:].strip())
        else:
            yield ("p", line)


def build_pdf():
    text = SOURCE.read_text(encoding="utf-8")

    styles = getSampleStyleSheet()
    title = ParagraphStyle(
        "TitleCustom",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=21,
        leading=25,
        textColor=colors.HexColor("#12345b"),
        spaceAfter=10,
    )
    h2 = ParagraphStyle(
        "Heading2Custom",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#12345b"),
        spaceBefore=8,
        spaceAfter=5,
    )
    h3 = ParagraphStyle(
        "Heading3Custom",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=11.5,
        leading=14,
        textColor=colors.HexColor("#234a75"),
        spaceBefore=5,
        spaceAfter=3,
    )
    body = ParagraphStyle(
        "BodyCustom",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=14,
        spaceAfter=4,
    )
    bullet = ParagraphStyle(
        "BulletCustom",
        parent=body,
        leftIndent=14,
        firstLineIndent=-8,
    )

    story = []
    for kind, value in parse_markdown_lines(text):
        if kind == "blank":
            story.append(Spacer(1, 0.12 * cm))
        elif kind == "h1":
            story.append(Paragraph(value, title))
        elif kind == "h2":
            story.append(Paragraph(value, h2))
        elif kind == "h3":
            story.append(Paragraph(value, h3))
        elif kind == "bullet":
            story.append(Paragraph(f"• {value}", bullet))
        else:
            story.append(Paragraph(value, body))

    doc = SimpleDocTemplate(
        str(OUTPUT),
        pagesize=A4,
        leftMargin=1.8 * cm,
        rightMargin=1.8 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        title="FMCG Distribution Analytics Project Report",
    )
    doc.build(story)


if __name__ == "__main__":
    build_pdf()
