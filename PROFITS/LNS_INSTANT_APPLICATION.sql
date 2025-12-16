create table LNS_INSTANT_APPLICATION
(
    IMAGE_TYPE_2      VARCHAR(20) not null,
    IMAGE_2           BLOB(1048576),
    CATEGORY_ID_2     CHAR(4)     not null,
    FILENAME_2        CHAR(75)    not null,
    DESCRIPTION_2     CHAR(100),
    COMMENTS_2        CHAR(100),
    IMAGE_TYPE_1      VARCHAR(20) not null,
    IMAGE_1           BLOB(1048576),
    CATEGORY_ID_1     CHAR(4)     not null,
    FILENAME_1        CHAR(75)    not null,
    DESCRIPTION_1     CHAR(100),
    COMMENTS_1        CHAR(100),
    ID_NO             CHAR(20),
    ISSUE_AUTHORITY   VARCHAR(30),
    ISSUE_DATE        DATE,
    IN_REPLACE_FLAG   CHAR(1),
    DEP_ACC_TYPE      INTEGER,
    DEP_PRODUCT_ID    INTEGER,
    CUST_ID_TYPE      INTEGER,
    ID_COUNTRY        CHAR(10),
    INSTANT_MECHANISM SMALLINT,
    TRX_CURRENCY      INTEGER,
    TRX_USR           CHAR(8),
    TRX_UNIT          INTEGER,
    TRX_DATE          DATE,
    APPLICATION_ID    DECIMAL(18) not null
        constraint PK_INSTANT_APP
            primary key,
    PROGRAM_ID        INTEGER,
    CUST_ID           INTEGER,
    CUST_TYPE         CHAR(1),
    TITLE             CHAR(6),
    FIRST_NAME        CHAR(20),
    SURNAME           CHAR(70),
    SEX               CHAR(1),
    DATE_OF_BIRTH     DATE,
    MOBILE_TEL        VARCHAR(15),
    E_MAIL            CHAR(64),
    ADDRESS_1         VARCHAR(40),
    ADDRESS_2         VARCHAR(40),
    CITY              CHAR(30),
    ZIP_CODE          CHAR(10),
    TELEPHONE_1       CHAR(15),
    APPLICATION_STS   CHAR(1),
    ACCOUNT           CHAR(40),
    UPDATE_TMSTAMP    TIMESTAMP(6),
    INSERT_TMSTAMP    TIMESTAMP(6),
    NON_REGISTERED    CHAR(1),
    DEP_CLOAN         INTEGER,
    FATHER_SURNAME    CHAR(40),
    FATHER_NAME       CHAR(20),
    MOTHER_NAME       CHAR(20),
    MOTHER_SURNAME    CHAR(40),
    EMPLOYER          CHAR(40),
    FIN_RANGE         DECIMAL(15),
    OCCUPATION_NUM    INTEGER,
    PROF_STATUS       INTEGER,
    ACITVITY_TYPE     INTEGER,
    INCOME_LVL        INTEGER,
    FINANCIAL_SECTOR  INTEGER,
    WORK_ADD          CHAR(40),
    WORK_CITY         CHAR(30),
    WORK_ZIP_CODE     CHAR(10)
);

comment on column LNS_INSTANT_APPLICATION.ID_NO is 'Customers id number.';

comment on column LNS_INSTANT_APPLICATION.ISSUE_AUTHORITY is 'It is the issue authority of the id.';

comment on column LNS_INSTANT_APPLICATION.ISSUE_DATE is 'Date at which other passport was issued.';

comment on column LNS_INSTANT_APPLICATION.CUST_ID is 'It is a unique customer identification number given automatically by the system.';

comment on column LNS_INSTANT_APPLICATION.CUST_TYPE is 'It is the type of the customer that is individual or company.';

comment on column LNS_INSTANT_APPLICATION.TITLE is 'It is the customer''s title, i.e.: Mr., Mrs., Dr., etc';

comment on column LNS_INSTANT_APPLICATION.FIRST_NAME is 'It is the first name of a customer (in case of anindividual person) or the abbreviation of a company stitle.';

comment on column LNS_INSTANT_APPLICATION.SURNAME is 'It is the surname of a customer (in case ofindividuals) or the full company title.';

comment on column LNS_INSTANT_APPLICATION.SEX is 'It is the flag that indicates if the customer is a : -1 Male - 2 Female.';

comment on column LNS_INSTANT_APPLICATION.DATE_OF_BIRTH is 'It is the date that the customer was born for individual customer or the company was founded for a company customer.';

comment on column LNS_INSTANT_APPLICATION.MOBILE_TEL is 'It is the mobile telephone of the customer.';

comment on column LNS_INSTANT_APPLICATION.E_MAIL is 'Customer''s email address.';

comment on column LNS_INSTANT_APPLICATION.ADDRESS_1 is 'Address line 1.';

comment on column LNS_INSTANT_APPLICATION.ADDRESS_2 is 'Address line 2 or Plot / Street No :';

comment on column LNS_INSTANT_APPLICATION.ZIP_CODE is 'VERSION 1.2';

comment on column LNS_INSTANT_APPLICATION.TELEPHONE_1 is 'It is the telephone number that the bank uses tocontact with the customer.';

comment on column LNS_INSTANT_APPLICATION.APPLICATION_STS is 'It is a flag indicating that the customer is: - 0active - 1 to be finalized - 9 inactive. After thecustomer is opened, it takes the indicator 1.Thismeans that the customer can use the bank products, buta batch process will check his uniqueness in theenti';

comment on column LNS_INSTANT_APPLICATION.UPDATE_TMSTAMP is 'TMSTAMP';

comment on column LNS_INSTANT_APPLICATION.INSERT_TMSTAMP is 'TMSTAMP';

