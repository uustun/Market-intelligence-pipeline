-- Companies House Market Intelligence Schema
# Core design

CREATE TABLE companies (
    company_number VARCHAR(20) PRIMARY KEY,
    company_name NVARCHAR(255) NOT NULL,
    company_status VARCHAR(50),
    incorporation_date DATE,
    company_type VARCHAR(50),
    created_at DATETIME2 DEFAULT SYSDATETIME()
);

CREATE TABLE company_addresses (
    address_id INT IDENTITY(1,1) PRIMARY KEY,
    company_number VARCHAR(20) NOT NULL,
    locality NVARCHAR(100),
    region NVARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    CONSTRAINT fk_address_company
        FOREIGN KEY (company_number)
        REFERENCES companies(company_number)
);

CREATE TABLE sic_codes (
    sic_code VARCHAR(10) PRIMARY KEY,
    description NVARCHAR(255)
);

CREATE TABLE company_sic (
    company_number VARCHAR(20) NOT NULL,
    sic_code VARCHAR(10) NOT NULL,
    CONSTRAINT pk_company_sic PRIMARY KEY (company_number, sic_code),
    CONSTRAINT fk_cs_company FOREIGN KEY (company_number)
        REFERENCES companies(company_number),
    CONSTRAINT fk_cs_sic FOREIGN KEY (sic_code)
        REFERENCES sic_codes(sic_code)
);

CREATE TABLE ingestion_log (
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_timestamp DATETIME2 DEFAULT SYSDATETIME(),
    records_inserted INT,
    source VARCHAR(100),
    status VARCHAR(20)
);
