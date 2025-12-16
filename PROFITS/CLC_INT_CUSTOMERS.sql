create table CLC_INT_CUSTOMERS
(
    BP_ID                          VARCHAR(20)  not null,
    BP_TYPE                        CHAR(1)      not null,
    BP_TITLE                       VARCHAR(10),
    BP_NAME                        VARCHAR(100),
    BP_FULLNAME                    VARCHAR(150),
    BP_LEGALFORM                   CHAR(2),
    BP_TAXBOOKCATEGORY             CHAR(2),
    BP_CORPORATEREGNO              CHAR(35),
    BP_CREDITRATING                VARCHAR(50),
    BP_HIGHRISK                    CHAR(1),
    BP_CREDITRATINGDATE            DATE,
    BP_CREDITRATINGRREV            VARCHAR(50),
    BP_PD                          DECIMAL(18, 3),
    BP_FORECASTSAMT                VARCHAR(50),
    BP_QUARTERFORECASTREF          VARCHAR(50),
    BP_LEGALENDDATE                DATE,
    BP_CREDITRATLMT                DECIMAL(18, 3),
    BP_CREDITLMTENDDATE            DATE,
    BP_INDUSTRYID                  VARCHAR(50),
    BP_INDUSTRYCODE                VARCHAR(50),
    BP_TAXCLEARENDDATE             DATE,
    BP_TAXINSAWARENDDATE           DATE,
    BP_BRANCHID                    CHAR(4),
    BP_FIRSTNAME                   VARCHAR(100),
    BP_MIDDLENAME                  VARCHAR(100),
    BP_LASTNAME                    VARCHAR(100) not null,
    BP_LATIN                       VARCHAR(150),
    BP_FATHERNAME                  VARCHAR(150),
    BP_MOTHERNAME                  VARCHAR(150),
    BP_MOTHERLASTNAME              VARCHAR(150),
    BP_SPOUSENAME                  VARCHAR(150),
    BP_CUSTTAXNO                   VARCHAR(20),
    BP_SSN                         VARCHAR(9)   not null,
    BP_IDTYPE                      VARCHAR(2),
    BP_IDNUM                       VARCHAR(30),
    BP_IDISSUER                    VARCHAR(30),
    BP_IDISSUEDATE                 DATE,
    BP_BASELANGUAGE                VARCHAR(30),
    BP_GENDER                      CHAR(1),
    BP_MARITALSTATUS               CHAR(1),
    BP_NOOFCHILDREN                SMALLINT,
    BP_DATEOFBIRTH                 DATE,
    BP_DATEOFDEATH                 DATE,
    BP_CITIZENSHIP                 CHAR(3),
    BP_NATIONALITY                 CHAR(3),
    BP_JOB                         CHAR(3),
    BP_EMPLOYERNAME                VARCHAR(100),
    BP_CATEGORY1                   VARCHAR(9),
    BP_CATEGORY2                   CHAR(2),
    BP_CLASS1                      CHAR(1),
    BP_CLASS2                      CHAR(1),
    BP_MAXBUCKET                   INTEGER,
    BP_DATEOPEN                    DATE,
    BP_LEGALPROTECTFLAG            SMALLINT,
    BP_LEGALPROTECTSTATUS          CHAR(1),
    BP_LEGALPROTECTREQUESTDATE     DATE,
    BP_LEGALPROTECTTEMPORDERDATE   DATE,
    BP_LEGALPROTECTENTRYDATE       DATE,
    BP_LEGALPROTECTINACTIVECOMPANY SMALLINT,
    BP_LEGALPROTECTNOTES           VARCHAR(1024),
    BP_BANKRUPTCYFLAG              SMALLINT,
    BP_BANKRUPTCYSTARTEDFLAG       SMALLINT,
    BP_BANKRUPTCYINACTIVECOMPANY   SMALLINT,
    BP_BANKRUPTCYSTATUS            SMALLINT,
    BP_BANKRUPTCYAPPLDATE          DATE,
    BP_BANKRUPTCYDECISIONDATE      DATE,
    BP_BANKRUPTCYSTOPPMTDATE       DATE,
    BP_BANKRUPTCYADVO              VARCHAR(100),
    BP_BANKRUPTCYNOTES             VARCHAR(100),
    BP_DATADATE                    DATE         not null,
    BP_EXPORTDATE                  DATE         not null,
    TMSTAMP_CUSTOMER               TIMESTAMP(6),
    TMSTAMP_CUST_IMAGE             TIMESTAMP(6),
    TMSTAMP_CUST_ADVANCES_INFO     TIMESTAMP(6),
    TMSTAMP_CUS_DTL_HISTORY        TIMESTAMP(6),
    TMSTAMP_CUST_ADD_INFO          TIMESTAMP(6),
    OLD_BP_ID                      VARCHAR(20),
    BP_INCOME                      DECIMAL(18, 3),
    BP_COUNTRY_OF_RESIDENCE        VARCHAR(100),
    BP_AGE                         INTEGER,
    BP_IS_BANKRUPTED               CHAR(1),
    BP_IS_DECEASED                 CHAR(1),
    BP_INCORPORATION_DATE          DATE,
    BP_IS_UPDATED                  CHAR(1),
    constraint PK_CLC_INT_CUSTOME
        primary key (BP_EXPORTDATE, BP_ID)
);

comment on table CLC_INT_CUSTOMERS is 'This table will contain records, each one representing a business party, i.e. distinct individual or legal entity (e.g. company, organisation). It should contain all customers appearing as debtors or otherwise related directly to accounts (e.g. guarantor';

comment on column CLC_INT_CUSTOMERS.BP_ID is 'Business PartyID (Unique Key) - CUSTOMER.CUST_ID';

comment on column CLC_INT_CUSTOMERS.BP_TYPE is 'Business Party Type (1=Individual, 2=Corporate) -> CUSTOMER.CUST_TYPE';

comment on column CLC_INT_CUSTOMERS.BP_TITLE is 'Customer Title/ Prefix. ( Mr., Mrs., Miss, Dr. Prof.) - CUSTOMER.TITLE';

comment on column CLC_INT_CUSTOMERS.BP_NAME is 'Company name for corporate - CUSTOMER.SURNAME';

comment on column CLC_INT_CUSTOMERS.BP_FULLNAME is 'Company full name - CUSTOMER.SURNAME';

comment on column CLC_INT_CUSTOMERS.BP_LEGALFORM is 'Company Legal Form - GENERIC_HEADER (LEGAL)';

comment on column CLC_INT_CUSTOMERS.BP_TAXBOOKCATEGORY is 'Tax Book Category (A, B, etc) - CUST_IMAGE.BOOK_INDICATOR';

comment on column CLC_INT_CUSTOMERS.BP_CORPORATEREGNO is 'Corporate Registration Number (ARMAE/) - OTHER_ID.ID_NO (VIA GENERIC_DETAILS)';

comment on column CLC_INT_CUSTOMERS.BP_CREDITRATING is 'Credit Rating - CUST_ADVANCES_INFO.FK_CREDIT_RATINSER';

comment on column CLC_INT_CUSTOMERS.BP_HIGHRISK is 'High Risk Customer (0=No,1=Yes) - N/A';

comment on column CLC_INT_CUSTOMERS.BP_CREDITRATINGDATE is 'Review Date/  - CUST_ADVANCES_INFO.CLASSIF_DATE';

comment on column CLC_INT_CUSTOMERS.BP_CREDITRATINGRREV is 'Previous Credit Rating/ Credit Rating - CUS_DTL_HISTORY.CLASSIFICATION';

comment on column CLC_INT_CUSTOMERS.BP_PD is 'Probability of Default (PD)/  -';

comment on column CLC_INT_CUSTOMERS.BP_FORECASTSAMT is 'Forecasts/';

comment on column CLC_INT_CUSTOMERS.BP_QUARTERFORECASTREF is 'Quarter Reference Forecast/   -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALENDDATE is 'Legal End Date//    - CUSTOMER.LEGAL_EXPIRE_DATE';

comment on column CLC_INT_CUSTOMERS.BP_CREDITRATLMT is 'Credit Rating Limit/   - CUSTOMER.LIMIT';

comment on column CLC_INT_CUSTOMERS.BP_CREDITLMTENDDATE is 'Credit Limit End Date/     -';

comment on column CLC_INT_CUSTOMERS.BP_INDUSTRYID is 'Industry ID/  - customer_category for profession (parameter FINSC)';

comment on column CLC_INT_CUSTOMERS.BP_INDUSTRYCODE is 'Industry Code/  - customer_category for profession (parameter CCODE)';

comment on column CLC_INT_CUSTOMERS.BP_TAXCLEARENDDATE is 'Tax Clearance End Date/    - CUST_ADD_INFO. TAX_END_DATE';

comment on column CLC_INT_CUSTOMERS.BP_TAXINSAWARENDDATE is 'Insurance Awareness End Date/    - CUST_ADD_INFO. INSURANCE_END_DATE';

comment on column CLC_INT_CUSTOMERS.BP_BRANCHID is 'Branch ID - CUSTOMER.FKUNIT_BELONG, CP_DELTA_BRANCHES.DELTA_BRANCH';

comment on column CLC_INT_CUSTOMERS.BP_FIRSTNAME is 'Customer First Name - CUSTOMER.FIRST_NAME';

comment on column CLC_INT_CUSTOMERS.BP_MIDDLENAME is 'Customer Middle Name - CUSTOMER.MIDDLE_NAME';

comment on column CLC_INT_CUSTOMERS.BP_LASTNAME is 'Customer Last Name - CUSTOMER.SURNAME';

comment on column CLC_INT_CUSTOMERS.BP_LATIN is 'Customer Name in Latin - CUSTOMER.LATIN_FIRSTNAME';

comment on column CLC_INT_CUSTOMERS.BP_FATHERNAME is 'Customers Father Name - CUSTOMER.FATHER_NAME';

comment on column CLC_INT_CUSTOMERS.BP_MOTHERNAME is 'Customers Mother Name - CUSTOMER.MOTHER_NAME';

comment on column CLC_INT_CUSTOMERS.BP_MOTHERLASTNAME is 'Customers Mother Maiden Name - CUSTOMER.MOTHER_SURNAME';

comment on column CLC_INT_CUSTOMERS.BP_SPOUSENAME is 'Customers Spouse Name - CUSTOMER.SPOUSE_NAME';

comment on column CLC_INT_CUSTOMERS.BP_CUSTTAXNO is 'VAT Number - other_afm (OTHER_AFM.FK_CUSTOMERCUST_ID = CUSTOMER.CUST_ID)';

comment on column CLC_INT_CUSTOMERS.BP_SSN is 'Social Security Number/ - OTHER_ID.ID_NO (VIA GENERIC_DETAILS) GENERIC_DETAIL where PARAMETER_TYPE = ''OIDTP'' AND LATIN_DESC = 09';

comment on column CLC_INT_CUSTOMERS.BP_IDTYPE is 'Customer Identification Type - OTHER_ID.ID_NO (VIA GENERIC_DETAILS)GENERIC_DETAIL. LATIN_DESC (2) for PARAMETER_TYPE = ''OIDTP''';

comment on column CLC_INT_CUSTOMERS.BP_IDNUM is 'Customer Identification Number - OTHER_ID.ID_NO';

comment on column CLC_INT_CUSTOMERS.BP_IDISSUER is 'Issuer of Identity - OTHER_ID.ISSUE_AUTHORITY';

comment on column CLC_INT_CUSTOMERS.BP_IDISSUEDATE is 'Issue date - OTHER_ID.ISSUE_DATE';

comment on column CLC_INT_CUSTOMERS.BP_BASELANGUAGE is 'Speaking Language - GENERIC_DETAILDESCRPTION where PARAMETER_TYPE = ''COMLA''';

comment on column CLC_INT_CUSTOMERS.BP_GENDER is 'Sex (M=Male, F=Female and NULL for corporates) - CUSTOMER.SEX';

comment on column CLC_INT_CUSTOMERS.BP_MARITALSTATUS is 'Marital Status - customer_category for family status(parameter FALST)';

comment on column CLC_INT_CUSTOMERS.BP_NOOFCHILDREN is 'Dependents - Personal details  Number of children - CUSTOMER.NUM_OF_CHILDREN';

comment on column CLC_INT_CUSTOMERS.BP_DATEOFBIRTH is 'Date Of Birth \ Incorporation - CUSTOMER.DATE_OF_BIRTH';

comment on column CLC_INT_CUSTOMERS.BP_DATEOFDEATH is 'Date of Death - CUSTOMER.STATUS_DATE (For Death)';

comment on column CLC_INT_CUSTOMERS.BP_CITIZENSHIP is 'Customer Citizenship - customer_category for citizenship(parameter CITIZ)';

comment on column CLC_INT_CUSTOMERS.BP_NATIONALITY is 'Customer Nationality - customer_category for nationality(parameter NATION)';

comment on column CLC_INT_CUSTOMERS.BP_JOB is 'Customer Occupation (Profession Code) - customer_category for type of business (parameter PROFF)';

comment on column CLC_INT_CUSTOMERS.BP_EMPLOYERNAME is 'Employer/ - CUSTOMER.EMPLOYER';

comment on column CLC_INT_CUSTOMERS.BP_CATEGORY1 is 'Customer Category/  - CUSTOMER_CATEGORY.FK_CATEGORYCATEGOR';

comment on column CLC_INT_CUSTOMERS.BP_CATEGORY2 is '  - CUST_ADVANCES_INFO. OPEN_CLASSIF_CUST';

comment on column CLC_INT_CUSTOMERS.BP_CLASS1 is 'Customer Type/  ( /  ) - Customer CUSTOMER_TYPE1-Individual Else Legal Type Code';

comment on column CLC_INT_CUSTOMERS.BP_CLASS2 is 'Additional customer Classification -';

comment on column CLC_INT_CUSTOMERS.BP_MAXBUCKET is 'Customer Maximum Bucket -';

comment on column CLC_INT_CUSTOMERS.BP_DATEOPEN is 'Customer Date Open//   - CUSTOMER.CUST_OPEN_DATE';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTFLAG is 'Legal Protection Flag (FLAG: 0=No, 1=Yes) (e.g. .3869/2010) -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTSTATUS is 'Legal Protection Status -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTREQUESTDATE is 'Legal Protection Request Date/  (e.g. .3869/2010) -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTTEMPORDERDATE is 'Legal Protection Temporary Order Date/.   (e.g. .3869/2010) -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTENTRYDATE is 'Legal Protection Entry Date/  (e.g. .3869/2010) -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTINACTIVECOMPANY is 'Legal Protection Inactive Company/  (FLAG: 0=No, 1=Yes) (e.g. .3869/2010) -';

comment on column CLC_INT_CUSTOMERS.BP_LEGALPROTECTNOTES is 'Notes regarding legal protection of customer -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYFLAG is 'Bankruptcy Status (FLAG: 0=No, 1=Yes) -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYSTARTEDFLAG is 'Bankruptcy process started (FLAG: 0=No, 1=Yes) -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYINACTIVECOMPANY is 'Bankruptcy Inactive Business/-  -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYSTATUS is 'Bankruptcy Status - CUSTOMER.BLACKLISTED_IND =1 ';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYAPPLDATE is 'Bankruptcy Application Date/-  -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYDECISIONDATE is 'Bankruptcy Decision Date/-  -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYSTOPPMTDATE is 'Bankruptcy Stop Pay Date/-   -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYADVO is 'Bankruptcy-Advocate/- -';

comment on column CLC_INT_CUSTOMERS.BP_BANKRUPTCYNOTES is 'BankruptcyOther Information / Notes -';

comment on column CLC_INT_CUSTOMERS.BP_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_CUSTOMERS.BP_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

