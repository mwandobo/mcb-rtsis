create table WEB_MNT_QRY
(
    TRX_DATE          DATE         not null,
    TRX_USER          CHAR(8)      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    TERMINAL_NUMBER   CHAR(99)     not null,
    FIRSTNAME         VARCHAR(32)  not null,
    LASTNAME          VARCHAR(32)  not null,
    CUSTNO            VARCHAR(16),
    STREET            VARCHAR(32),
    ZIP               VARCHAR(7),
    TOWN              VARCHAR(28),
    H_COUNTRY         VARCHAR(3),
    S_COUNTRY         VARCHAR(3),
    CUSY              VARCHAR(8),
    PROFESSION        VARCHAR(32),
    BRANCH            VARCHAR(32),
    BIRTHDATE         VARCHAR(8),
    CUSTCONTACT       VARCHAR(8),
    NAT_COUNTRY       VARCHAR(3),
    BRANCH_OFFICE     VARCHAR(10),
    CUST_TYPE         VARCHAR(1),
    EMPLNO            VARCHAR(16),
    PASSPORT          VARCHAR(17),
    COUNTRY_OF_BIRTH  VARCHAR(32),
    PLACE_OF_BIRTH    VARCHAR(32),
    QRY_TMSTAMP       TIMESTAMP(6),
    ACTION_ENTRY_DESC CHAR(20),
    constraint IXU_CUS_048
        primary key (TRX_DATE, TRX_USER, TERMINAL_NUMBER, TMSTAMP)
);

create unique index IXN_CUS_005
    on WEB_MNT_QRY (TRX_DATE, TRX_USER, TERMINAL_NUMBER, LASTNAME, FIRSTNAME);

