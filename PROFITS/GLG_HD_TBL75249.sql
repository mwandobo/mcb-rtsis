create table GLG_HD_TBL75249
(
    HD_ID           INTEGER not null
        constraint IXU_GL_033
            primary key,
    USER_SN         SMALLINT,
    TO_UNIT         INTEGER,
    FROM_UNIT       INTEGER,
    TO_CURR_ID      INTEGER,
    FROM_CURR_ID    INTEGER,
    CURR_TRX_DATE   DATE,
    TO_DATE         DATE,
    FROM_DATE       DATE,
    CUR_TIMSTAMP    TIMESTAMP(6),
    USR             CHAR(8),
    TAX_REG_NO      CHAR(10),
    FROM_ACCOUNT_ID CHAR(21),
    TO_ACCOUNT_ID   CHAR(21),
    BANK_NAME       CHAR(40)
);

