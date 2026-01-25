-- Raw W2 forms table
-- Stores IRS Form W-2 Wage and Tax Statement data from PDF extractions
--
-- Data Model Design:
-- - Core fields (employee, employer, wages, taxes) as typed columns for easy querying
-- - State/local tax info as JSON (supports 0-2 states per W2)
-- - Optional boxes (12, 14) as JSON (only stored when present)
-- - This balances queryability with flexibility for sparse/variable data

CREATE TABLE IF NOT EXISTS raw.w2_forms (
    -- Tax year and identifiers
    tax_year INTEGER NOT NULL,
    employee_ssn VARCHAR NOT NULL,
    employer_ein VARCHAR NOT NULL,
    control_number VARCHAR,

    -- Employee information
    employee_first_name VARCHAR NOT NULL,
    employee_last_name VARCHAR NOT NULL,
    employee_address VARCHAR,

    -- Employer information
    employer_name VARCHAR NOT NULL,
    employer_address VARCHAR,

    -- Core wage and tax amounts (boxes 1-6)
    wages DECIMAL(18, 2) NOT NULL,
    federal_income_tax DECIMAL(18, 2) NOT NULL,
    social_security_wages DECIMAL(18, 2),
    social_security_tax DECIMAL(18, 2),
    medicare_wages DECIMAL(18, 2),
    medicare_tax DECIMAL(18, 2),

    -- Additional compensation (boxes 7-8)
    social_security_tips DECIMAL(18, 2),
    allocated_tips DECIMAL(18, 2),

    -- Benefits (box 10-11)
    dependent_care_benefits DECIMAL(18, 2),
    nonqualified_plans DECIMAL(18, 2),

    -- Box 13 checkboxes
    is_statutory_employee BOOLEAN DEFAULT FALSE,
    is_retirement_plan BOOLEAN DEFAULT FALSE,
    is_third_party_sick_pay BOOLEAN DEFAULT FALSE,

    -- State and local tax information (boxes 15-20)
    -- Stored as JSON array to handle multiple states (typically 1-2)
    -- Example: [{"state": "CA", "employer_state_id": "1234567", "state_wages": 100000.00, "state_income_tax": 5000.00}]
    state_local_info JSON,

    -- Optional boxes (12, 14)
    -- Stored as JSON object for flexibility with sparse data
    -- Example: {"box_12_codes": {"D": "19500.00", "DD": "8450.00"}, "box_14_other": "Union dues"}
    optional_boxes JSON,

    -- Metadata
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Primary key ensures one W2 per employee per employer per year per source file
    PRIMARY KEY (tax_year, employee_ssn, employer_ein, source_file)
);
