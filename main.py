from ReportGen import ReportGenerator
import ConnectDatabase as db
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Any
import duckdb
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor

class DataQualityChecker:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.setup_logging()
        self.rules = {
            'numeric_ranges': {
                'Unit_price': (0, 1000),
                'Quantity': (0, 100),
                'Rating': (0, 10),
                'gross_margin_pct': (0, 100)
            },
            'categorical_values': {
                'Branch': ['A', 'B', 'C'],
                'Customer_type': ['Member', 'Normal'],
                'Gender': ['Male', 'Female'],
                'Payment': ['Cash', 'Credit card', 'Ewallet']
            }
        }

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)


    def check_single_column(self, column: str) -> tuple:
        aconn=duckdb.connect("my_database.db", read_only=True)
        try:
            query = f"SELECT CAST(COUNT(*) - COUNT({column}) AS INTEGER) FROM sales"
            result = aconn.execute(query).fetchone()
            return (column, result[0] if result else 0)
        except Exception as e:
            self.logger.error(f"Error checking column {column}: {e}")
            return (column, 0)

    def check_missing_values(self) -> Dict[str, int]:
        aconn = duckdb.connect("my_database.db", read_only=True)
        columns = aconn.execute("SELECT * FROM sales LIMIT 0").description
        column_names = [desc[0] for desc in columns]

        with ThreadPoolExecutor() as executor:
            args = [(db.connect(), col) for col in column_names]
            results = executor.map(check_column_missing, args)

        return dict(results)


    def check_numeric_ranges(self) -> Dict[str, Dict]:
        db_path = 'my_database.db'
        args = [(db_path, col, range_[0], range_[1])
                for col, range_ in self.rules['numeric_ranges'].items()]

        with Pool(processes=cpu_count()) as pool:
            results = pool.map(check_column_range, args)

        return dict(results)

    def check_categorical_values(self) -> Dict[str, List[str]]:
        aconn=duckdb.connect("my_database.db", read_only=True)
        results = {}
        for column, valid_values in self.rules['categorical_values'].items():
            values_str = ", ".join([f"'{v}'" for v in valid_values])
            query = f"""
           SELECT DISTINCT {column}
           FROM sales
           WHERE {column} NOT IN ({values_str})
           """
            result = aconn.execute(query).fetchdf()
            results[column] = result[column].tolist() if not result.empty else []
        return results

    def check_data_consistency(self) -> Dict[str, List[Dict]]:
        aconn = duckdb.connect("my_database.db", read_only=True)
        consistency_checks = {
            'total_calculation': """
               SELECT "Invoice_ID", Total,
                      ("Unit_price" * Quantity * (1 + 0.05)) as calculated_total,
                      ABS(Total - ("Unit_price" * Quantity * (1 + 0.05))) as difference
               FROM sales
               WHERE ABS(Total - ("Unit_price" * Quantity * (1 + 0.05))) > 0.01
           """,
            'future_dates': """
               SELECT "Invoice_ID", Date
               FROM sales
               WHERE Date > CURRENT_DATE
           """
        }

        results = {}
        for check_name, query in consistency_checks.items():
            result = aconn.execute(query).fetchdf()
            results[check_name] = result.to_dict('records')
        return results

    def generate_quality_report_(self) -> Dict:
        report = {
            'missing_values': self.check_missing_values(),
            'numeric_ranges': self.check_numeric_ranges(),
            'categorical_values': self.check_categorical_values(),
            'consistency_checks': self.check_data_consistency()
        }

        summary_query = """
       SELECT
           COUNT(*) as total_records,
           COUNT(DISTINCT "Invoice_ID") as unique_invoices,
           COUNT(DISTINCT "Product_line") as unique_products,
           AVG(Rating) as avg_rating,
           AVG("gross_margin_pct") as avg_margin
       FROM sales
       """
        summary = self.conn.execute(summary_query).fetchdf()
        report['summary_statistics'] = summary.to_dict('records')[0]
        return report

    def generate_quality_report(self) -> Dict:
        with ThreadPoolExecutor() as executor:
            # Use existing class connection for checks
            futures = {
                'missing_values': executor.submit(lambda: self.check_missing_values()),
                'numeric_ranges': executor.submit(lambda: self.check_numeric_ranges()),
                'categorical_values': executor.submit(lambda: self.check_categorical_values()),
                'consistency_checks': executor.submit(lambda: self.check_data_consistency())
            }

            # Use same connection for summary
            def get_summary():
                query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT "Invoice_ID") as unique_invoices,
                    COUNT(DISTINCT "Product_line") as unique_products,
                    AVG(Rating) as avg_rating,
                    AVG("gross_margin_pct") as avg_margin
                FROM sales
                """
                result = self.conn.execute(query).fetchdf()
                records = result.to_dict('records')
                return records[0] if records else {
                    'total_records': 0,
                    'unique_invoices': 0,
                    'unique_products': 0,
                    'avg_rating': 0,
                    'avg_margin': 0
                }

            summary_future = executor.submit(get_summary)

            # Collect results
            report = {key: future.result() for key, future in futures.items()}
            report['summary_statistics'] = summary_future.result()

        return report

def auto_correct_data(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:

    corrections = {
        'trim_whitespace': """
           UPDATE sales 
           SET 
               "Invoice_ID" = TRIM("Invoice_ID"),
               Branch = TRIM(Branch),
               City = TRIM(City),
               "Customer_type" = TRIM("Customer_type"),
               Gender = TRIM(Gender),
               "Product_line" = TRIM("Product_line"),
               Payment = TRIM(Payment)
       """,
        'fix_case_customer_type': """
           UPDATE sales 
           SET "Customer_type" = 
               CASE 
                   WHEN LOWER("Customer_type") = 'member' THEN 'Member'
                   WHEN LOWER("Customer_type") = 'normal' THEN 'Normal'
                   ELSE "Customer_type"
               END
       """,
        'fix_case_gender': """
           UPDATE sales 
           SET Gender = 
               CASE 
                   WHEN LOWER(Gender) = 'male' THEN 'Male'
                   WHEN LOWER(Gender) = 'female' THEN 'Female'
                   ELSE Gender
               END
       """
    }

    correction_counts = {}
    for correction_name, query in corrections.items():
        try:
            result = conn.execute(query)
            correction_counts[correction_name] = result.fetchone()[0]
        except Exception as e:
            logging.error(f"Error in correction {correction_name}: {str(e)}")
            correction_counts[correction_name] = -1

    return correction_counts


def check_column_missing(args):
    conn, column = args
    query = f"SELECT COUNT(*) - COUNT({column}) FROM sales"
    result = conn.execute(query).fetchone()
    conn.close()
    return column, result[0] if result else 0

def check_column_range(args):
    db_path, column, min_val, max_val = args
    conn = duckdb.connect(db_path, read_only=True)
    query = f"""
    SELECT 
        COUNT(*) as outlier_count,
        MIN({column}),
        MAX({column})
    FROM sales 
    WHERE {column} < {min_val} OR {column} > {max_val}
    """
    result = conn.execute(query).fetchone()
    conn.close()
    return column, {
        'outlier_count': result[0],
        'min_value': result[1],
        'max_value': result[2]
    }


def check_payment_consistency(
        sales_conn: duckdb.DuckDBPyConnection,
        payments_conn: duckdb.DuckDBPyConnection,
        mismatch_threshold: float = 0.01
) -> Dict[str, List[Dict]]:


    duplicates_query = """
        SELECT 
            Invoice_ID, 
            COUNT(*) as payment_count
        FROM payments
        GROUP BY Invoice_ID
        HAVING COUNT(*) > 1
    """
    duplicates_df = payments_conn.execute(duplicates_query).fetchdf()
    duplicate_payments = duplicates_df.to_dict('records')


    sales_df = sales_conn.execute("""
        SELECT 
            Invoice_ID,
            Total as sales_total
        FROM sales
    """).fetchdf()

    payments_df = payments_conn.execute("""
        SELECT
            Invoice_ID,
            Total as payment_total
        FROM payments
    """).fetchdf()


    merged_df = pd.merge(
        sales_df,
        payments_df,
        on='Invoice_ID',
        how='inner',  # or 'left', depending on your use-case
        validate='many_to_many'
    )


    merged_df['difference'] = merged_df['sales_total'] - merged_df['payment_total']

    mismatch_df = merged_df[merged_df['difference'].abs() > mismatch_threshold]

    amount_mismatch = mismatch_df.to_dict('records')


    return {
        'duplicate_payments': duplicate_payments,
        'amount_mismatch': amount_mismatch
    }


def main():
    import faulthandler
    faulthandler.enable()

    sales_conn = db.connect_write()
    db.reset(sales_conn,'supermarket_sales.csv','sales')
    payments_conn = db.connect_write_payments()
    db.reset(payments_conn,'payments.csv','payments')
    sales_conn.close()
    payments_conn.close()

    sales_conn = db.connect()
    payments_conn = db.connect_payments()

    checker = DataQualityChecker(sales_conn)
    quality_report = checker.generate_quality_report()
    report_gen = ReportGenerator(quality_report)
    report_gen.generate_pdf_report()
    report_gen.export_report_to_excel(quality_report)

    # Payment discrepancies
    payment_discrepancies = check_payment_consistency(sales_conn, payments_conn, mismatch_threshold=0.01)
    report_gen.generate_payment_report(payment_discrepancies)

    print("Payment Discrepancies:")
    print(payment_discrepancies)

    sales_conn.close()
    payments_conn.close()
    # Corrections
    sales_conn = db.connect_write()
    correction_results = auto_correct_data(sales_conn)
    sales_conn.close()
if __name__ == "__main__":
    main()