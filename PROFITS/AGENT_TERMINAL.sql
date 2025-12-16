create table AGENT_TERMINAL
(
    USER_CODE               CHAR(8),
    ENTRY_STATUS            CHAR(1),
    TERMINAL_TYPE           VARCHAR(40),
    LOCATION                CHAR(80),
    INSERTION_BRANCH        INTEGER,
    INSERTION_USER          CHAR(8),
    INSERTION_DATE          DATE,
    INSERTION_TMSTAMP       TIMESTAMP(6),
    INSERTION_AUTHORIZED_BY CHAR(8),
    UPDATE_BRANCH           INTEGER,
    UPDATE_USER             CHAR(8),
    UPDATE_DATE             DATE,
    UPDATE_TMSTAMP          TIMESTAMP(6),
    UPDATE_AUTHORIZED_BY    CHAR(8),
    FK_USRCODE              CHAR(8) not null,
    FK_AGENT_CUST_ID        INTEGER not null,
    FK_AGENT_CHANNEL_ID     INTEGER not null,
    FK_GENERIC_HEADPAR      CHAR(5),
    FK_GENERIC_DETASER      INTEGER,
    PRFT_ACCOUNT            CHAR(40)
);

