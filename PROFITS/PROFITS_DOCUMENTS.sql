create table PROFITS_DOCUMENTS
(
    FK_GH_RQTP1       CHAR(5) not null,
    FK_GD_RQTP1       INTEGER not null,
    FK_GH_RQTP2       CHAR(5) not null,
    FK_GD_RQTP2       INTEGER not null,
    FK_GH_RQTP3       CHAR(5) not null,
    FK_GD_RQTP3       INTEGER not null,
    RULE_SYSTEM       SMALLINT,
    CHECK_AFTER       SMALLINT,
    UPDATE_UNIT       INTEGER,
    INSERT_UNIT       INTEGER,
    RULE_ID           DECIMAL(12),
    UPDATE_DATE       DATE,
    INSERT_DATE       DATE,
    TMSTAMP           TIMESTAMP(6),
    OPT_CORRESPONDENT CHAR(1),
    OPT_CORPORATE     CHAR(1),
    OPT_INDIVIDUAL    CHAR(1),
    DOC_SCAN_FLAG     CHAR(1),
    INSERT_USER       CHAR(8),
    UPDATE_USER       CHAR(8),
    ENTRY_COMMENTS    CHAR(254),
    DOC_CATEGORY_ID   CHAR(4),
    constraint IXU_PRD_022
        primary key (FK_GH_RQTP1, FK_GD_RQTP1, FK_GH_RQTP2, FK_GD_RQTP2, FK_GH_RQTP3, FK_GD_RQTP3)
);

