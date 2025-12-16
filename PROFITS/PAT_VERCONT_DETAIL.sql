create table PAT_VERCONT_DETAIL
(
    RECORD_DATE               DATE        not null,
    SEQ_ID                    DECIMAL(10) not null,
    TYPE_OF_ENTITY            CHAR(2)     not null,
    OBJECT_ID                 DECIMAL(10) not null,
    REASON                    CHAR(2)     not null,
    DESCRIPTION               VARCHAR(500),
    FK_PAT_VERCONT_RUN_SEQ_ID INTEGER,
    FK_PAT_VERCONT_RUN_DATE   DATE,
    constraint PATVDPK1
        primary key (REASON, OBJECT_ID, TYPE_OF_ENTITY, SEQ_ID, RECORD_DATE)
);

create unique index PATVDI1
    on PAT_VERCONT_DETAIL (FK_PAT_VERCONT_RUN_SEQ_ID, FK_PAT_VERCONT_RUN_DATE);

