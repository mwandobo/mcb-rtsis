create table CMS_CARD_LIMIT
(
    CARD_LIMIT_SN   DECIMAL(10) not null,
    UTILISED_AMNT   DECIMAL(15, 2),
    ENTRY_STATUS    CHAR(1),
    LAST_RESET_DATE DATE,
    NEXT_RESET_DATE DATE,
    FK_LIMIT_HD_CD  CHAR(15),
    FK_LIMIT_DT_SN  DECIMAL(10),
    FK_CARD_SN      DECIMAL(10) not null,
    LAST_USED_DATE  DATE,
    constraint PK_CARD_LIMIT
        primary key (CARD_LIMIT_SN, FK_CARD_SN)
);

create unique index I0001047
    on CMS_CARD_LIMIT (FK_CARD_SN);

create unique index I0001142
    on CMS_CARD_LIMIT (FK_LIMIT_HD_CD, FK_LIMIT_DT_SN);

