create table ACC_CUST_CHANNEL
(
    FK_DISTR_CHANNEID INTEGER     not null,
    FK0CUSTOMER_CHAFK INTEGER     not null,
    CP_AGREEMENT_NO   DECIMAL(10) not null,
    ACCOUNT_NUMBER    DECIMAL(11) not null,
    CP_CD             SMALLINT,
    ACC_CD            SMALLINT,
    ALIAS_ACCOUNT     SMALLINT,
    SEQ_ORDER_NBR     SMALLINT,
    TIMSTAMP          TIMESTAMP(6),
    ACTIVATION_DATE   DATE,
    ALGORITHM_IND     CHAR(1),
    DEP_TPP_FLAG      CHAR(1),
    FK_UNIT_CATEGORID CHAR(8),
    NICKNAME          CHAR(16),
    "prompt_4"        CHAR(30),
    TPP_FIELD_2       CHAR(30),
    TPP_FIELD_1       CHAR(30),
    "prompt_3"        CHAR(30),
    "prompt_1"        CHAR(30),
    TPP_FIELD_4       CHAR(30),
    TPP_FIELD_3       CHAR(30),
    "prompt_2"        CHAR(30),
    PROMPT_1          CHAR(30),
    PROMPT_2          CHAR(30),
    PROMPT_3          CHAR(30),
    PROMPT_4          CHAR(30),
    constraint IXU_CIS_168
        primary key (FK_DISTR_CHANNEID, FK0CUSTOMER_CHAFK, CP_AGREEMENT_NO, ACCOUNT_NUMBER)
);

