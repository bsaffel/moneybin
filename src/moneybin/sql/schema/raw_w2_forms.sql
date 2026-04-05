-- Raw W2 forms table
-- Stores IRS Form W-2 Wage and Tax Statement data from PDF extractions
--
-- Data Model Design:
-- - Core fields (employee, employer, wages, taxes) as typed columns for easy querying
-- - State/local tax info as JSON (supports 0-2 states per W2)
-- - Optional boxes (12, 14) as JSON (only stored when present)
-- - This balances queryability with flexibility for sparse/variable data

CREATE TABLE IF NOT EXISTS raw.w2_forms (
    tax_year INTEGER NOT NULL, -- Tax year this W-2 covers; part of primary key
    employee_ssn VARCHAR NOT NULL, -- Employee Social Security Number; part of primary key
    employer_ein VARCHAR NOT NULL, -- Employer Identification Number; part of primary key
    control_number VARCHAR, -- Box d: employer-assigned control number; optional
    employee_first_name VARCHAR NOT NULL, -- Employee first name as printed on the W-2
    employee_last_name VARCHAR NOT NULL, -- Employee last name as printed on the W-2
    employee_address VARCHAR, -- Employee mailing address as printed on the W-2; NULL when not extractable
    employer_name VARCHAR NOT NULL, -- Employer name as printed on the W-2
    employer_address VARCHAR, -- Employer mailing address as printed on the W-2; NULL when not extractable
    wages DECIMAL(18, 2) NOT NULL, -- Box 1: federal taxable wages and tips
    federal_income_tax DECIMAL(18, 2) NOT NULL, -- Box 2: federal income tax withheld
    social_security_wages DECIMAL(18, 2), -- Box 3: wages subject to Social Security tax (capped at annual wage base)
    social_security_tax DECIMAL(18, 2), -- Box 4: Social Security tax withheld
    medicare_wages DECIMAL(18, 2), -- Box 5: wages subject to Medicare tax (no cap)
    medicare_tax DECIMAL(18, 2), -- Box 6: Medicare tax withheld
    social_security_tips DECIMAL(18, 2), -- Box 7: tips reported to employer subject to Social Security tax
    allocated_tips DECIMAL(18, 2), -- Box 8: tips allocated by employer when reported tips seem low
    dependent_care_benefits DECIMAL(18, 2), -- Box 10: employer-provided dependent care benefits
    nonqualified_plans DECIMAL(18, 2), -- Box 11: distributions from nonqualified deferred compensation plans
    is_statutory_employee BOOLEAN DEFAULT FALSE, -- Box 13: worker classified as statutory employee for Social Security purposes
    is_retirement_plan BOOLEAN DEFAULT FALSE, -- Box 13: employee participated in an employer retirement plan this year
    is_third_party_sick_pay BOOLEAN DEFAULT FALSE, -- Box 13: sick pay was paid by a third-party insurer
    state_local_info JSON, -- Boxes 15-20 as JSON array; supports 1-2 states per W-2. Schema: [{state, employer_state_id, state_wages, state_income_tax}]
    optional_boxes JSON, -- Boxes 12 and 14 as JSON; only present when reported. Schema: {box_12_codes: {code: amount}, box_14_other: description}
    source_file VARCHAR NOT NULL, -- Path to the PDF file this record was extracted from; part of primary key
    extracted_at TIMESTAMP NOT NULL, -- Timestamp when the PDF was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (tax_year, employee_ssn, employer_ein, source_file)
);
