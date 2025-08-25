# Data Sources Strategy

## Institution-Specific Extraction Approach

### Priority 1: Plaid API (Recommended)

**Supported Institutions:**

- ✅ **Wells Fargo**: Full support (checking, savings, credit cards)
- ✅ **Chase**: Full support (all account types)
- ✅ **Capital One**: Full support (banking and credit products)
- ✅ **Fidelity**: Investment accounts and 401(k) support
- ✅ **E*TRADE**: Brokerage and retirement accounts
- ❌ **Goldman Sachs Wealth Management**: Limited/No retail API access
- ❌ **QuickBooks**: Separate API (QuickBooks API, not Plaid)

**Implementation:**

```python
# Plaid integration for supported institutions
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest

def get_plaid_transactions(access_token, start_date, end_date):
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date
    )
    response = client.transactions_get(request)
    return response['transactions']
```

### Priority 2: Direct Bank APIs / Yodlee

**Alternative API Options:**

#### Wells Fargo

- **Gateway API**: Limited to business customers
- **Fallback**: Manual CSV export from online banking

#### Chase

- **Chase for Business API**: Business accounts only
- **Fallback**: Manual CSV export or PDF statement processing

#### Capital One

- **DevExchange API**: Limited access, application required
- **Fallback**: Manual CSV export

#### Goldman Sachs Wealth Management

- **Marcus API**: Limited to Marcus savings products
- **Private Wealth**: No public API - requires manual processing
- **Approach**: PDF statement processing + manual CSV export

#### QuickBooks

- **QuickBooks Online API**: Full access to accounting data
- **Implementation**: Direct API integration separate from banking APIs

```python
# QuickBooks API integration
from intuitlib.client import AuthClient
from quickbooks import QuickBooks

def get_quickbooks_data(access_token):
    client = QuickBooks(
        auth_client=auth_client,
        refresh_token=refresh_token,
        company_id=company_id
    )
    return client.query("SELECT * FROM Item")
```

### Priority 3: Manual CSV Processing

**All Institutions Fallback:**

#### CSV Export Process

1. **Wells Fargo**: Online Banking → Account Activity → Export → CSV
2. **Chase**: Account Details → Download Activity → Comma Delimited
3. **Capital One**: Account Details → Download Transactions → CSV
4. **Fidelity**: Portfolio → History → Download → CSV
5. **E*TRADE**: Accounts → History → Export → CSV
6. **Goldman Sachs**: Private Wealth portal → Export (if available)

#### Standardization Pipeline

```python
# CSV standardization for different bank formats
def standardize_bank_csv(file_path, bank_type):
    bank_parsers = {
        'wells_fargo': parse_wells_fargo_csv,
        'chase': parse_chase_csv,
        'capital_one': parse_capital_one_csv,
        'fidelity': parse_fidelity_csv,
        'etrade': parse_etrade_csv
    }
    return bank_parsers[bank_type](file_path)
```

### Priority 4: PDF Statement Processing

**For Institutions Without CSV Export:**

#### Modern OCR Strategy

**Primary Tool: pdfplumber (2024 recommendation)**

- Excellent table extraction
- Handles complex layouts
- Active development and maintenance
- Best performance for financial statements

**Backup Tools:**

- **tabula-py**: For table-heavy documents
- **camelot-py**: For complex table structures
- **PyMuPDF**: For text-heavy documents

```python
# PDF processing with pdfplumber
import pdfplumber
import pandas as pd

def extract_bank_statement(pdf_path):
    transactions = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if is_transaction_table(table):
                    transactions.extend(parse_transaction_table(table))
    return pd.DataFrame(transactions)
```

## Tax Document Processing

### IRS.gov Documents

**Supported Forms:**

- Form 1040 (Individual Income Tax Return)
- Schedule A (Itemized Deductions)
- Schedule B (Interest and Ordinary Dividends)
- Schedule C (Profit or Loss from Business)
- Form W-2 (Wage and Tax Statement)
- Form 1099 variants (1099-INT, 1099-DIV, 1099-MISC, etc.)

**OCR Strategy:**

```python
# Tax form processing with form-specific templates
def extract_tax_form(pdf_path, form_type):
    form_extractors = {
        '1040': extract_form_1040,
        'w2': extract_w2,
        '1099': extract_1099_variants
    }
    return form_extractors[form_type](pdf_path)

def extract_form_1040(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        # Extract specific fields by coordinates/patterns
        data = {
            'total_income': extract_field_by_pattern(pdf, r'Total income.*(\d+,?\d*)'),
            'taxable_income': extract_field_by_pattern(pdf, r'Taxable income.*(\d+,?\d*)'),
            'total_tax': extract_field_by_pattern(pdf, r'Total tax.*(\d+,?\d*)')
        }
    return data
```

### Georgia Tax Center

**State-Specific Processing:**

- Georgia Form 500 (Individual Income Tax Return)
- Georgia Schedule 1 (Additional Income and Adjustments)
- Property tax documents

**Approach:**

- PDF processing with state-specific templates
- Manual fallback for complex forms
- Validation against known Georgia tax form structures

## Implementation Priority Matrix

| Institution | Priority 1 (Plaid) | Priority 2 (Direct API) | Priority 3 (CSV) | Priority 4 (PDF) |
|-------------|-------------------|------------------------|------------------|------------------|
| Wells Fargo | ✅ Full Support | ❌ Business Only | ✅ Available | ✅ Fallback |
| Chase | ✅ Full Support | ❌ Business Only | ✅ Available | ✅ Fallback |
| Capital One | ✅ Full Support | ❌ Limited Access | ✅ Available | ✅ Fallback |
| Fidelity | ✅ Full Support | ❌ No Public API | ✅ Available | ✅ Fallback |
| E*TRADE | ✅ Full Support | ❌ No Public API | ✅ Available | ✅ Fallback |
| Goldman Sachs | ❌ Not Supported | ❌ Private Wealth Only | ⚠️ Limited | ✅ Primary Method |
| QuickBooks | ❌ Separate API | ✅ Full API Access | ✅ Export Available | ❌ Not Applicable |
| IRS.gov | ❌ No API | ❌ No API | ❌ No Export | ✅ PDF Only |
| Georgia Tax | ❌ No API | ❌ No API | ❌ No Export | ✅ PDF Only |

## Error Handling & Fallback Strategy

### Automated Fallback Chain

1. **Plaid API** → Connection issues or unsupported account
2. **Direct Bank API** → Rate limits or access denied
3. **Manual CSV Import** → User uploads exported files
4. **PDF Processing** → OCR extraction from statements
5. **Manual Entry** → Web interface for manual data input

### Data Validation Pipeline

```python
def validate_extracted_data(data, source_type):
    validations = [
        validate_date_ranges,
        validate_amount_formats,
        validate_required_fields,
        detect_duplicates,
        cross_reference_totals
    ]

    for validation in validations:
        if not validation(data):
            raise DataValidationError(f"Validation failed for {source_type}")

    return data
```

This strategy provides comprehensive coverage of all target institutions with appropriate fallback mechanisms and modern tooling.
