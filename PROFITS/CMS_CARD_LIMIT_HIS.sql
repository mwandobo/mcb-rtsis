create table CMS_CARD_LIMIT_HIS
(
    TMSTAMP             TIMESTAMP(6)   not null,
    CARD_LIMIT_SN       DECIMAL(10)    not null,
    UTILISED_AMNT       DECIMAL(15, 2) not null,
    LAST_RESET_DATE     DATE,
    NEXT_RESET_DATE     DATE,
    FK_LIMIT_HD_CD      CHAR(15),
    FK_LIMIT_DT_SN      DECIMAL(10),
    FK_CARD_HIS_TMSTAMP TIMESTAMP(6),
    LAST_USED_DATE      DATE,
    constraint PK_CRDLIMIT_HIS
        primary key (CARD_LIMIT_SN, TMSTAMP)
);

create unique index I0001059
    on CMS_CARD_LIMIT_HIS (FK_LIMIT_HD_CD, FK_LIMIT_DT_SN);

create unique index I0011058
    on CMS_CARD_LIMIT_HIS (FK_CARD_HIS_TMSTAMP);

