import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.units import inch
from typing import Dict, List
import os
from datetime import datetime

class ReportGenerator:
    def __init__(self, report_data: Dict):
        """Initialize with the quality check report data."""
        self.report_data = report_data
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def generate_csv_reports(self, output_dir: str = 'reports'):
        """Generate CSV reports for each aspect of the data quality check."""
        os.makedirs(output_dir, exist_ok=True)
        base_filename = f"{output_dir}/quality_report_{self.timestamp}"

        # Missing values report
        pd.DataFrame([self.report_data['missing_values']]).to_csv(
            f"{base_filename}_missing_values.csv")

        # Numeric ranges report
        numeric_ranges_df = pd.DataFrame.from_dict(
            self.report_data['numeric_ranges'],
            orient='index'
        )
        numeric_ranges_df.to_csv(f"{base_filename}_numeric_ranges.csv")

        # Categorical values report
        categorical_df = pd.DataFrame([
            {'Column': col, 'Invalid_Values': ','.join(map(str, values))}
            for col, values in self.report_data['categorical_values'].items()
        ])
        categorical_df.to_csv(f"{base_filename}_categorical_values.csv", index=False)

        # Consistency checks report
        for check_name, issues in self.report_data['consistency_checks'].items():
            if issues:
                pd.DataFrame(issues).to_csv(
                    f"{base_filename}_consistency_{check_name}.csv",
                    index=False
                )

        # Summary statistics
        pd.DataFrame([self.report_data['summary_statistics']]).to_csv(
            f"{base_filename}_summary.csv")

        return base_filename

    def generate_pdf_report(self, output_dir: str = 'reports'):
        """Generate a comprehensive PDF report."""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/quality_report_{self.timestamp}.pdf"
        doc = SimpleDocTemplate(filename, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30
        )
        elements.append(Paragraph("Data Quality Report", title_style))
        elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                                styles['Normal']))
        elements.append(Spacer(1, 20))

        # Summary Statistics
        elements.append(Paragraph("Summary Statistics", styles['Heading2']))
        summary_data = [[k, str(v)] for k, v in self.report_data['summary_statistics'].items()]
        summary_table = Table(
            [['Metric', 'Value']] + summary_data,
            colWidths=[4*inch, 3*inch]
        )
        summary_table.setStyle(self._get_table_style())
        elements.append(summary_table)
        elements.append(Spacer(1, 20))

        # Missing Values
        elements.append(Paragraph("Missing Values Analysis", styles['Heading2']))
        missing_data = [[k, str(v)] for k, v in self.report_data['missing_values'].items()
                       if v > 0]
        if missing_data:
            missing_table = Table(
                [['Column', 'Missing Count']] + missing_data,
                colWidths=[4*inch, 3*inch]
            )
            missing_table.setStyle(self._get_table_style())
            elements.append(missing_table)
        else:
            elements.append(Paragraph("No missing values found.", styles['Normal']))
        elements.append(Spacer(1, 20))

        # Numeric Range Violations
        elements.append(Paragraph("Numeric Range Violations", styles['Heading2']))
        for col, stats in self.report_data['numeric_ranges'].items():
            if stats['outlier_count'] > 0:
                elements.append(Paragraph(f"Column: {col}", styles['Heading3']))
                range_data = [[k, str(v)] for k, v in stats.items()]
                range_table = Table(
                    [['Metric', 'Value']] + range_data,
                    colWidths=[4*inch, 3*inch]
                )
                range_table.setStyle(self._get_table_style())
                elements.append(range_table)
                elements.append(Spacer(1, 10))
        elements.append(Spacer(1, 10))

        # Categorical Value Violations
        elements.append(Paragraph("Categorical Value Violations", styles['Heading2']))
        cat_violations = False
        for col, invalid_values in self.report_data['categorical_values'].items():
            if invalid_values:
                cat_violations = True
                elements.append(Paragraph(
                    f"Column: {col}\nInvalid Values: {', '.join(map(str, invalid_values))}",
                    styles['Normal']
                ))
                elements.append(Spacer(1, 10))
        if not cat_violations:
            elements.append(Paragraph("No categorical value violations found.",
                                   styles['Normal']))
        elements.append(Spacer(1, 20))

        # Consistency Checks
        elements.append(Paragraph("Data Consistency Issues", styles['Heading2']))
        for check_name, issues in self.report_data['consistency_checks'].items():
            if issues:
                elements.append(Paragraph(f"{check_name}", styles['Heading3']))
                if issues:
                    # Get columns from first record
                    columns = list(issues[0].keys())
                    # Create table with all records
                    table_data = [columns]  # Header row
                    for issue in issues:
                        table_data.append([str(issue[col]) for col in columns])

                    table = Table(table_data, colWidths=[2 * inch] * len(columns))
                    table.setStyle(self._get_table_style())
                    elements.append(table)
                elements.append(Spacer(1, 10))
            else:
                elements.append(Paragraph(f"No issues found for {check_name}", styles['Normal']))

        # Build PDF
        doc.build(elements)
        return filename

    def _get_table_style(self):
        """Return a consistent table style for the report."""
        return TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ])

    def generate_payment_report(self, discrepancies: Dict, output_dir: str = 'reports'):

        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/payment_discrepancies_{self.timestamp}.pdf"
        doc = SimpleDocTemplate(filename, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30
        )
        elements.append(Paragraph("Payment Discrepancy Report", title_style))
        elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                                  styles['Normal']))
        elements.append(Spacer(1, 20))

        # Amount Mismatches Table
        elements.append(Paragraph("Amount Mismatches", styles['Heading2']))
        if discrepancies['amount_mismatch']:
            # Get all column names from first record
            columns = list(discrepancies['amount_mismatch'][0].keys())
            # Create header row
            table_data = [columns]
            # Add all records
            for record in discrepancies['amount_mismatch']:
                table_data.append([str(record[col]) for col in columns])

            table = Table(table_data, colWidths=[2 * inch] * len(columns))
            table.setStyle(self._get_table_style())
            elements.append(table)
        else:
            elements.append(Paragraph("No amount mismatches found.", styles['Normal']))
        elements.append(Spacer(1, 20))

        # Duplicate Payments Table
        elements.append(Paragraph("Duplicate Payments", styles['Heading2']))
        if discrepancies['duplicate_payments']:
            # Get columns from first record
            columns = list(discrepancies['duplicate_payments'][0].keys())
            # Create header row
            table_data = [columns]
            # Add all records
            for record in discrepancies['duplicate_payments']:
                table_data.append([str(record[col]) for col in columns])

            table = Table(table_data, colWidths=[3 * inch] * len(columns))
            table.setStyle(self._get_table_style())
            elements.append(table)
        else:
            elements.append(Paragraph("No duplicate payments found.", styles['Normal']))

        # Build PDF
        doc.build(elements)
        return filename

    def export_report_to_excel(report: Dict, filename: str = 'data_quality_report.xlsx'):
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Missing values
            pd.DataFrame([report['missing_values']]).to_excel(
                writer, sheet_name='Missing Values', index=False)

            # Numeric ranges - show all stats
            numeric_ranges_df = pd.DataFrame(report['numeric_ranges']).T
            numeric_ranges_df.to_excel(writer, sheet_name='Numeric Ranges')

            # Categorical values - show all violations
            categorical_df = pd.DataFrame([
                {'Column': col, 'Invalid Values': ', '.join(map(str, values))}
                for col, values in report['categorical_values'].items()
            ])
            categorical_df.to_excel(writer, sheet_name='Categorical Values', index=False)

            # Consistency checks - all records for each type of check
            for check_name, results in report['consistency_checks'].items():
                if results:
                    df = pd.DataFrame(results)
                    df.to_excel(writer, sheet_name=f'Consistency_{check_name}', index=False)

            # Summary statistics
            pd.DataFrame([report['summary_statistics']]).to_excel(
                writer, sheet_name='Summary Statistics', index=False)
