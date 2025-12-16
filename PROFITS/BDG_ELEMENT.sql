create table BDG_ELEMENT
(
    ID                CHAR(30) not null
        constraint IXU_GL_021
            primary key,
    STATUS            SMALLINT,
    TYPE              SMALLINT,
    FK4BDG_ELMNT_BHID SMALLINT,
    FK3BDG_ELMNT_BHID SMALLINT,
    FK2BDG_ELMNT_BHID SMALLINT,
    FK1BDG_ELMNT_BHID SMALLINT,
    FK0BDG_ELMNT_BHID SMALLINT,
    FK_BDG_ELMNT_BHID SMALLINT,
    MIN_RANGE         DECIMAL(5, 2),
    MAX_RANGE         DECIMAL(5, 2),
    TMSTMP            DATE,
    CNTR_RANGE        CHAR(1),
    CTG_ELMNT         CHAR(1),
    RATE_TYPE         CHAR(2),
    REAL_UPD_ELM_CD   CHAR(30),
    DESCRIPTION       CHAR(80),
    ANALYTICAL_DESCR  VARCHAR(500),
    ELEMENT_KIND      CHAR(2),
    MIN_LEVEL         CHAR(2),
    FK_USR_BUDGET     CHAR(8),
    FK_PROFILE_BUDGET CHAR(8),
    DEFINE_WAY        CHAR(2),
    UPDATE_USR        CHAR(8),
    UPDATE_UNIT       INTEGER,
    UPDATE_DATE       DATE,
    UPDATE_TMSTAMP    TIMESTAMP(6)
);

create unique index I0000509
    on BDG_ELEMENT (FK_BDG_ELMNT_BHID);

create unique index I0000511
    on BDG_ELEMENT (FK0BDG_ELMNT_BHID);

create unique index I0000513
    on BDG_ELEMENT (FK1BDG_ELMNT_BHID);

create unique index I0000515
    on BDG_ELEMENT (FK2BDG_ELMNT_BHID);

create unique index I0000517
    on BDG_ELEMENT (FK3BDG_ELMNT_BHID);

create unique index I0000519
    on BDG_ELEMENT (FK4BDG_ELMNT_BHID);

