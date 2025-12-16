create table H_CHEQ_ITEM
(
    FK_CHEQ_ITEM_CHID  INTEGER,
    FK_CURRENCYID_CURR INTEGER,
    VALIDITY_DATE      DATE,
    SNUM               SMALLINT,
    NUMBER3            SMALLINT,
    NUMBER9            SMALLINT,
    NUMBER8            SMALLINT,
    NUMBER7            SMALLINT,
    NUMBER6            SMALLINT,
    NUMBER5            SMALLINT,
    NUMBER4            SMALLINT,
    NUMBER2            SMALLINT,
    NUMBER1            SMALLINT,
    NUMBER0            SMALLINT,
    AMOUNT4            DECIMAL(15, 2),
    AMOUNT7            DECIMAL(15, 2),
    AMOUNT6            DECIMAL(15, 2),
    AMOUNT1            DECIMAL(15, 2),
    AMOUNT9            DECIMAL(15, 2),
    AMOUNT8            DECIMAL(15, 2),
    AMOUNT5            DECIMAL(15, 2),
    MAX_AMOUNT         DECIMAL(15, 2),
    MIN_AMOUNT         DECIMAL(15, 2),
    AMOUNT3            DECIMAL(15, 2),
    AMOUNT2            DECIMAL(15, 2),
    AMOUNT0            DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6),
    CONTINUATION       CHAR(1),
    ENTRY_STATUS       CHAR(1)
);

create unique index IXU_H_C_001
    on H_CHEQ_ITEM (FK_CHEQ_ITEM_CHID, FK_CURRENCYID_CURR, VALIDITY_DATE, SNUM);

