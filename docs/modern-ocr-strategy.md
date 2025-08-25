# Modern OCR Strategy for PDF Processing

## Primary Tool: pdfplumber (2024 Recommendation)

### Why pdfplumber?

- **Best for Financial Documents**: Excellent table and text extraction from structured PDFs
- **Active Development**: Regular updates and bug fixes in 2024
- **Financial Statement Optimized**: Handles banking and tax document layouts well
- **Python Native**: No Java dependencies (unlike tabula-py)
- **Precise Control**: Coordinate-based extraction for consistent form layouts

### Installation & Setup

```bash
uv pip install pdfplumber>=0.11.4
uv pip install pillow>=10.4.0  # For image processing
```

## Core Implementation Strategy

### 1. Tax Form Processing (IRS.gov, Georgia Tax Center)

#### Form 1040 Extraction

```python
import pdfplumber
import re
from typing import Dict, Optional

class Form1040Extractor:
    def __init__(self):
        # Define field patterns and coordinates for Form 1040
        self.field_patterns = {
            'total_income': r'Total income.*?(\d{1,3}(?:,\d{3})*)',
            'adjusted_gross_income': r'Adjusted gross income.*?(\d{1,3}(?:,\d{3})*)',
            'taxable_income': r'Taxable income.*?(\d{1,3}(?:,\d{3})*)',
            'total_tax': r'Total tax.*?(\d{1,3}(?:,\d{3})*)',
            'federal_tax_withheld': r'Federal income tax withheld.*?(\d{1,3}(?:,\d{3})*)'
        }

    def extract_form_1040(self, pdf_path: str) -> Dict[str, Optional[float]]:
        """Extract key fields from Form 1040"""
        extracted_data = {}

        with pdfplumber.open(pdf_path) as pdf:
            # Combine text from all pages
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"

            # Extract fields using patterns
            for field_name, pattern in self.field_patterns.items():
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    # Clean and convert to float
                    amount_str = match.group(1).replace(',', '')
                    extracted_data[field_name] = float(amount_str)
                else:
                    extracted_data[field_name] = None

        return extracted_data

    def extract_w2_forms(self, pdf_path: str) -> List[Dict]:
        """Extract W-2 data (multiple forms per PDF)"""
        w2_data = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # W-2 forms are typically one per page
                text = page.extract_text()

                # Extract employer info
                employer_match = re.search(r'Employer.*?([A-Z][A-Za-z\s&,\.]+)', text)

                # Extract wage and tax amounts
                wages_match = re.search(r'Wages.*?(\d{1,3}(?:,\d{3})*\.?\d*)', text)
                fed_tax_match = re.search(r'Federal income tax withheld.*?(\d{1,3}(?:,\d{3})*\.?\d*)', text)

                if wages_match:
                    w2_data.append({
                        'employer': employer_match.group(1) if employer_match else None,
                        'wages': float(wages_match.group(1).replace(',', '')),
                        'federal_tax_withheld': float(fed_tax_match.group(1).replace(',', '')) if fed_tax_match else 0
                    })

        return w2_data
```

#### Georgia Tax Forms

```python
class GeorgiaTaxExtractor:
    def extract_form_500(self, pdf_path: str) -> Dict[str, float]:
        """Extract Georgia Form 500 data"""
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text()

            # Georgia-specific patterns
            patterns = {
                'ga_adjusted_gross_income': r'Georgia adjusted gross income.*?(\d{1,3}(?:,\d{3})*)',
                'ga_tax_liability': r'Tax liability.*?(\d{1,3}(?:,\d{3})*)',
                'ga_tax_withheld': r'Georgia income tax withheld.*?(\d{1,3}(?:,\d{3})*)'
            }

            extracted = {}
            for field, pattern in patterns.items():
                match = re.search(pattern, text, re.IGNORECASE)
                extracted[field] = float(match.group(1).replace(',', '')) if match else 0

            return extracted
```

### 2. Bank Statement Processing

#### Multi-Bank Statement Processor

```python
class BankStatementProcessor:
    def __init__(self):
        self.bank_processors = {
            'wells_fargo': self._process_wells_fargo,
            'chase': self._process_chase,
            'capital_one': self._process_capital_one,
            'goldman_sachs': self._process_goldman_sachs
        }

    def detect_bank_type(self, pdf_path: str) -> str:
        """Auto-detect bank type from PDF content"""
        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text().lower()

            if 'wells fargo' in first_page_text:
                return 'wells_fargo'
            elif 'chase' in first_page_text or 'jpmorgan' in first_page_text:
                return 'chase'
            elif 'capital one' in first_page_text:
                return 'capital_one'
            elif 'goldman sachs' in first_page_text or 'marcus' in first_page_text:
                return 'goldman_sachs'
            else:
                return 'generic'

    def _process_wells_fargo(self, pdf_path: str) -> pd.DataFrame:
        """Process Wells Fargo statements"""
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Wells Fargo typically has transaction tables
                tables = page.extract_tables()

                for table in tables:
                    if self._is_transaction_table(table):
                        for row in table[1:]:  # Skip header
                            if len(row) >= 4 and row[0]:  # Date, Description, Amount, Balance
                                transactions.append({
                                    'date': self._parse_date(row[0]),
                                    'description': row[1],
                                    'amount': self._parse_amount(row[2]),
                                    'balance': self._parse_amount(row[3]) if row[3] else None,
                                    'bank': 'Wells Fargo'
                                })

        return pd.DataFrame(transactions)

    def _process_goldman_sachs(self, pdf_path: str) -> pd.DataFrame:
        """Process Goldman Sachs wealth management statements"""
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Goldman Sachs has different formats for different account types
                text = page.extract_text()
                tables = page.extract_tables()

                # Look for investment transactions
                if 'portfolio summary' in text.lower():
                    transactions.extend(self._extract_investment_transactions(tables))

                # Look for cash transactions
                if 'cash activity' in text.lower():
                    transactions.extend(self._extract_cash_transactions(tables))

        return pd.DataFrame(transactions)

    def _is_transaction_table(self, table: List[List[str]]) -> bool:
        """Identify if a table contains transaction data"""
        if not table or len(table) < 2:
            return False

        header = [cell.lower() if cell else '' for cell in table[0]]
        transaction_keywords = ['date', 'description', 'amount', 'balance', 'transaction']

        return any(keyword in ' '.join(header) for keyword in transaction_keywords)
```

### 3. Investment Account Processing (Fidelity, E*TRADE)

```python
class InvestmentStatementProcessor:
    def process_fidelity_statement(self, pdf_path: str) -> Dict[str, pd.DataFrame]:
        """Process Fidelity investment statements"""
        result = {
            'transactions': pd.DataFrame(),
            'positions': pd.DataFrame(),
            'performance': pd.DataFrame()
        }

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text().lower()
                tables = page.extract_tables()

                # Transaction history
                if 'transaction history' in text:
                    result['transactions'] = self._extract_investment_transactions(tables)

                # Account positions
                if 'account positions' in text or 'holdings' in text:
                    result['positions'] = self._extract_positions(tables)

                # Performance data
                if 'performance' in text or 'returns' in text:
                    result['performance'] = self._extract_performance(tables)

        return result

    def _extract_investment_transactions(self, tables: List[List[List[str]]]) -> pd.DataFrame:
        """Extract investment transaction data from tables"""
        transactions = []

        for table in tables:
            if len(table) > 1:
                header = [cell.lower() if cell else '' for cell in table[0]]

                # Look for investment transaction patterns
                if any(keyword in ' '.join(header) for keyword in ['symbol', 'shares', 'price', 'buy', 'sell']):
                    for row in table[1:]:
                        if len(row) >= 5:
                            transactions.append({
                                'date': self._parse_date(row[0]),
                                'symbol': row[1],
                                'transaction_type': row[2],
                                'shares': self._parse_float(row[3]),
                                'price': self._parse_float(row[4]),
                                'amount': self._parse_float(row[5]) if len(row) > 5 else None
                            })

        return pd.DataFrame(transactions)
```

## Backup Tools Strategy

### tabula-py for Complex Tables

```python
# Use when pdfplumber struggles with complex table structures
import tabula

def extract_with_tabula(pdf_path: str, pages: str = 'all') -> List[pd.DataFrame]:
    """Fallback to tabula-py for complex tables"""
    try:
        tables = tabula.read_pdf(pdf_path, pages=pages, multiple_tables=True)
        return tables
    except Exception as e:
        print(f"Tabula extraction failed: {e}")
        return []
```

### camelot-py for High-Precision Tables

```python
# Use for tables requiring highest precision
import camelot

def extract_with_camelot(pdf_path: str) -> List[pd.DataFrame]:
    """Use camelot for high-precision table extraction"""
    try:
        tables = camelot.read_pdf(pdf_path, flavor='lattice')
        return [table.df for table in tables]
    except Exception as e:
        print(f"Camelot extraction failed: {e}")
        return []
```

## Error Handling & Quality Assurance

### Extraction Validation

```python
class PDFExtractionValidator:
    def validate_tax_extraction(self, extracted_data: Dict, pdf_path: str) -> Dict[str, bool]:
        """Validate tax form extraction results"""
        validations = {}

        # Check for required fields
        required_fields = ['total_income', 'taxable_income', 'total_tax']
        validations['has_required_fields'] = all(
            field in extracted_data and extracted_data[field] is not None
            for field in required_fields
        )

        # Check for reasonable values
        if extracted_data.get('total_income'):
            validations['reasonable_income'] = 0 < extracted_data['total_income'] < 10_000_000

        # Check mathematical consistency
        if all(extracted_data.get(field) for field in ['total_income', 'taxable_income']):
            validations['income_consistency'] = extracted_data['taxable_income'] <= extracted_data['total_income']

        return validations

    def validate_bank_extraction(self, transactions_df: pd.DataFrame) -> Dict[str, bool]:
        """Validate bank statement extraction"""
        validations = {}

        # Check for required columns
        required_columns = ['date', 'description', 'amount']
        validations['has_required_columns'] = all(col in transactions_df.columns for col in required_columns)

        # Check data quality
        validations['no_null_dates'] = not transactions_df['date'].isnull().any()
        validations['reasonable_amounts'] = transactions_df['amount'].abs().max() < 1_000_000

        return validations
```

## Performance Optimization

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor
import os

class ParallelPDFProcessor:
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    def process_pdf_batch(self, pdf_paths: List[str]) -> Dict[str, pd.DataFrame]:
        """Process multiple PDFs in parallel"""
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs
            future_to_path = {
                executor.submit(self._process_single_pdf, path): path
                for path in pdf_paths
            }

            # Collect results
            for future in future_to_path:
                path = future_to_path[future]
                try:
                    results[path] = future.result()
                except Exception as e:
                    print(f"Error processing {path}: {e}")
                    results[path] = pd.DataFrame()

        return results
```

## Integration with Dagster Pipeline

```python
# pipelines/assets.py
from dagster import asset, get_dagster_logger
from src.extractors.pdf_extractor import TaxPDFExtractor, BankStatementProcessor

@asset(group_name="pdf_extraction")
def processed_tax_pdfs() -> pd.DataFrame:
    """Extract data from all tax PDFs in the input directory"""
    logger = get_dagster_logger()
    extractor = TaxPDFExtractor()

    pdf_dir = "data/raw/tax_pdfs/"
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]

    all_tax_data = []
    for pdf_file in pdf_files:
        try:
            pdf_path = os.path.join(pdf_dir, pdf_file)
            tax_data = extractor.extract_tax_form(pdf_path)
            tax_data['source_file'] = pdf_file
            all_tax_data.append(tax_data)
            logger.info(f"Successfully processed {pdf_file}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_file}: {e}")

    return pd.DataFrame(all_tax_data)

@asset(group_name="pdf_extraction")
def processed_bank_statements() -> pd.DataFrame:
    """Extract data from bank statement PDFs"""
    logger = get_dagster_logger()
    processor = BankStatementProcessor()

    statement_dir = "data/raw/bank_statements/"
    pdf_files = [f for f in os.listdir(statement_dir) if f.endswith('.pdf')]

    all_transactions = []
    for pdf_file in pdf_files:
        try:
            pdf_path = os.path.join(statement_dir, pdf_file)
            bank_type = processor.detect_bank_type(pdf_path)
            transactions = processor.bank_processors[bank_type](pdf_path)
            transactions['source_file'] = pdf_file
            all_transactions.append(transactions)
            logger.info(f"Successfully processed {pdf_file} as {bank_type}")
        except Exception as e:
            logger.error(f"Failed to process {pdf_file}: {e}")

    return pd.concat(all_transactions, ignore_index=True) if all_transactions else pd.DataFrame()
```

This modern OCR strategy provides robust, scalable PDF processing with pdfplumber as the primary tool, comprehensive fallback options, and strong integration with the Dagster pipeline architecture.
