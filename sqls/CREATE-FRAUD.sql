-- ============================================================
-- Fraud Incidence Reporting Table
-- ============================================================
CREATE TABLE FRAUD_INCIDENCE (
    -- Primary Key
    FRAUD_ID            BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY
                                        (START WITH 1 INCREMENT BY 1),

    -- Mandatory Fields
    REPORTING_DATE      TIMESTAMP       NOT NULL,                   -- ddmmyyhhmm → TIMESTAMP
    PRODUCT_PROCESS_INVOLVED VARCHAR(50) NOT NULL,                  -- Lookup: Table D154
    OCCURRENCE_DATE     TIMESTAMP       NOT NULL,                   -- ddmmyyhhmm → TIMESTAMP
    DETECTION_DATE      TIMESTAMP       NOT NULL,                   -- ddmmyyhhmm → TIMESTAMP
    DISCOVERY_MODEL     VARCHAR(50)     NOT NULL,                   -- Lookup: Table D155
    FSP_FRAUD_IDENTIFIED_DATE TIMESTAMP NOT NULL,                   -- ddmmyyhhmm → TIMESTAMP
    IDENTIFIED_FRAUD_CAUSES VARCHAR(50) NOT NULL,                   -- Lookup: Table D156
    FRAUD_AMOUNT        DECIMAL(20, 2)  NOT NULL,                   -- Numeric
    CLASSIFICATION      VARCHAR(50)     NOT NULL,                   -- Lookup: Table D157
    FRAUD_TYPE          VARCHAR(50)     NOT NULL,                   -- Lookup: Table D158
    FRAUD_NATURE        VARCHAR(50)     NOT NULL,                   -- Lookup: Table D159
    FRAUD_DESCRIPTION   VARCHAR(2000)   NOT NULL,                   -- Text
    VICTIM_NAME         VARCHAR(255)    NOT NULL,                   -- Text
    ACCOUNT_NUMBER      VARCHAR(50)     NOT NULL,                   -- Text (GL or account)
    BRANCH_CODE         VARCHAR(20)     NOT NULL,                   -- Alphanumeric
    ACTION_TAKEN        VARCHAR(500)    NOT NULL,                   -- Alphanumeric
    MITIGATION_MEASURES VARCHAR(2000)   NOT NULL,                   -- Text
    FRAUD_STATUS        VARCHAR(50)     NOT NULL,                   -- Lookup: Table D160
    FRAUDSTER_NAME      CLOB(10K)       NOT NULL,                   -- List (JSON/delimited)
    FRAUDSTER_COUNTRY   CLOB(5K)        NOT NULL,                   -- List (country codes)

    -- Optional Fields
    CONCLUDED_INVESTIGATION_DATE TIMESTAMP,                         -- Nullable
    EMP_NIN             CLOB(10K),                                  -- Nullable; list of NIns

    -- Audit columns
    CREATED_AT          TIMESTAMP       NOT NULL DEFAULT CURRENT TIMESTAMP,
    UPDATED_AT          TIMESTAMP       NOT NULL DEFAULT CURRENT TIMESTAMP,
    CREATED_BY          VARCHAR(100)    NOT NULL DEFAULT USER,

    -- Primary Key Constraint
    CONSTRAINT PK_FRAUD_INCIDENCE PRIMARY KEY (FRAUD_ID)
)
DATA CAPTURE NONE
IN USERSPACE1;

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IDX_FI_OCCURRENCE_DATE
    ON FRAUD_INCIDENCE (OCCURRENCE_DATE DESC);

CREATE INDEX IDX_FI_PRODUCT
    ON FRAUD_INCIDENCE (PRODUCT_PROCESS_INVOLVED);

CREATE INDEX IDX_FI_STATUS
    ON FRAUD_INCIDENCE (FRAUD_STATUS);

CREATE INDEX IDX_FI_CLASSIFICATION
    ON FRAUD_INCIDENCE (CLASSIFICATION);

CREATE INDEX IDX_FI_ACCOUNT
    ON FRAUD_INCIDENCE (ACCOUNT_NUMBER);

-- ============================================================
-- Comments
-- ============================================================
COMMENT ON TABLE FRAUD_INCIDENCE IS
    'Stores fraud incidence reports submitted to BOT (Bank of Tanzania)';

COMMENT ON COLUMN FRAUD_INCIDENCE.PRODUCT_PROCESS_INVOLVED IS 'productProcessInvolved – Lookup D154: cards, ATM, loan, treasury, POS, agent banking, etc.';
COMMENT ON COLUMN FRAUD_INCIDENCE.OCCURRENCE_DATE         IS 'occurrenceDate – Date the fraud actually occurred';
COMMENT ON COLUMN FRAUD_INCIDENCE.DETECTION_DATE          IS 'detectionDate – Date the fraud was detected';
COMMENT ON COLUMN FRAUD_INCIDENCE.CONCLUDED_INVESTIGATION_DATE IS 'concludedInvestigationDate – Optional; date investigation concluded by FSP';
COMMENT ON COLUMN FRAUD_INCIDENCE.DISCOVERY_MODEL         IS 'discoveryModel – Lookup D155: modalities that facilitated fraud discovery';
COMMENT ON COLUMN FRAUD_INCIDENCE.FSP_FRAUD_IDENTIFIED_DATE IS 'fspFraudIdentifiedDate – Date fraud reported by customer or identified by FSP';
COMMENT ON COLUMN FRAUD_INCIDENCE.IDENTIFIED_FRAUD_CAUSES IS 'identifiedFraudCauses – Lookup D156: weaknesses that caused the fraud';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUD_AMOUNT            IS 'fraudAmount – Actual amount involved in original currency';
COMMENT ON COLUMN FRAUD_INCIDENCE.CLASSIFICATION          IS 'classification – Lookup D157: internal or external fraud';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUD_TYPE              IS 'fraudType – Lookup D158: attempted or actual';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUD_NATURE            IS 'fraudNature – Lookup D159: e.g. card skimming, password sharing';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUD_DESCRIPTION       IS 'fraudDescription – Free text explanation of how fraud occurred';
COMMENT ON COLUMN FRAUD_INCIDENCE.VICTIM_NAME             IS 'victimName – Person or entity affected';
COMMENT ON COLUMN FRAUD_INCIDENCE.ACCOUNT_NUMBER          IS 'accountNumber – Affected account or GL number';
COMMENT ON COLUMN FRAUD_INCIDENCE.BRANCH_CODE             IS 'branchCode – Branch linked to affected customer';
COMMENT ON COLUMN FRAUD_INCIDENCE.ACTION_TAKEN            IS 'actionTaken – Immediate corrective measures taken';
COMMENT ON COLUMN FRAUD_INCIDENCE.MITIGATION_MEASURES     IS 'mitigationMeasures – Long-term measures to resolve the problem';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUD_STATUS            IS 'fraudStatus – Lookup D160: Refunded, Referred to BOT, Court, Closed, Under Investigation';
COMMENT ON COLUMN FRAUD_INCIDENCE.EMP_NIN                 IS 'empNin – Optional; NIN(s) of internal staff involved (stored as delimited list or JSON array)';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUDSTER_NAME          IS 'fraudsterName – Name(s) of external fraudster(s); list if multiple';
COMMENT ON COLUMN FRAUD_INCIDENCE.FRAUDSTER_COUNTRY       IS 'fraudsterCountry – Country code(s) per country lookup table; list matching fraudsterName order';