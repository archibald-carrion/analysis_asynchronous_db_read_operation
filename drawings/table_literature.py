from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT

def create_table_pdf(filename="async_io_summary.pdf"):
    # Create PDF with landscape orientation for better table fit
    doc = SimpleDocTemplate(filename, pagesize=landscape(letter),
                           rightMargin=30, leftMargin=30,
                           topMargin=30, bottomMargin=30)
    
    # Container for the 'Flowable' objects
    elements = []
    
    # Define styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=14,
        textColor=colors.HexColor('#2C3E50'),
        spaceAfter=20,
        alignment=TA_CENTER
    )
    
    # Add title
    title = Paragraph("Summary of Asynchronous I/O Techniques in Database Systems", title_style)
    elements.append(title)
    elements.append(Spacer(1, 0.2*inch))
    
    # Create table data
    data = [
        # Header row
        ['Study', 'Date', 'Database System', 'Async vs Sync', 'Hardware Platform', 
         'Database Size', 'Primary Evaluation Metric'],
        # Data rows
        ['Axboe [1]', '2019', 'io_uring design', 'Async only', 'Linux kernel', 
         'N/A', 'I/O performance, CPU overhead'],
        ['Mehdi et al. [2]', 'Jul 2023', 'ScaleDB (in-memory)', 'Async only', 
         'Multi-core servers', 'In-memory', 'Throughput (QPS/TPS), Abort rate'],
        ['Pestka et al. [3]', 'Nov 2024', 'Theoretical analysis', 'Async only', 
         'Modern SSDs', 'N/A', 'IOPS scaling, CPU sensitivity'],
        ['Chen et al. [4]', 'Nov 2024', 'Redis (in-memory)', 'Async only', 
         'Ryzen 7 + NVMe', 'In-memory datasets', 'Throughput (ops/s), Persistence time'],
        ['Xiao et al. [5]', 'Jul 2025', 'FlashANNS (ANNS)', 'Async only', 
         'NVMe SSD', 'Billion-scale vectors', 'Throughput (QPS), Recall'],
        ['Our Study', '2025', 'PostgreSQL 18', 'Both compared', 
         'VM + Laptop', '1GB-100GB', 'TPC-H metrics (Query execution time)']
    ]
    
    # Create table with wider last two columns
    table = Table(data, colWidths=[1*inch, 0.7*inch, 1.4*inch, 1.2*inch, 
                                   1.2*inch, 1.2*inch, 2.3*inch])
    
    # Style the table
    table_style = TableStyle([
        # Header styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, 0), 12),
        
        # Data rows styling
        ('BACKGROUND', (0, 1), (-1, -2), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#2C3E50')),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('TOPPADDING', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ('LEFTPADDING', (0, 1), (-1, -1), 6),
        ('RIGHTPADDING', (0, 1), (-1, -1), 6),
        
        # "Our Study" row highlighting
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#3498DB')),
        ('TEXTCOLOR', (0, -1), (-1, -1), colors.whitesmoke),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        
        # Grid
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('LINEBELOW', (0, 0), (-1, 0), 2, colors.HexColor('#34495E')),
        
        # Alternating row colors (except last row)
        ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.beige, colors.white]),
        
        # Vertical alignment
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ])
    
    table.setStyle(table_style)
    elements.append(table)
    
    # Build PDF
    doc.build(elements)
    print(f"PDF created successfully: {filename}")

if __name__ == "__main__":
    create_table_pdf()