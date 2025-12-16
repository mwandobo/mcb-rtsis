create table PAT_WINCONTROL
(
    UID0                 DECIMAL(10)  not null
        constraint PATWCPK1
            primary key,
    UID_OF_OWNER         DECIMAL(10)  not null,
    WINCONTROL_ID        INTEGER      not null,
    GEN_SEQ_NBR          INTEGER      not null,
    OLD_UID              DECIMAL(10)  not null,
    OLD_UID_OF_OWNER     DECIMAL(10)  not null,
    XLEFT                INTEGER      not null,
    XRIGHT               INTEGER      not null,
    YBOTTOM              INTEGER      not null,
    YTOP                 INTEGER      not null,
    NAME                 CHAR(32)     not null,
    PROMPT               VARCHAR(60),
    HAS_INPUT            CHAR(1),
    STATUS               CHAR(1)      not null,
    LAST_CHANGED         TIMESTAMP(6) not null,
    WINCONTROL_ID_SOURCE CHAR(1)      not null,
    GEN_WINCONTROL_TYPE  INTEGER      not null,
    AU_WINCONTROL_TYPE   CHAR(1)      not null,
    IS_BTNGROCCURENCE    CHAR(1)      not null,
    IS_GROUPITEM         CHAR(1)      not null,
    IS_WINFIELD          CHAR(1)      not null,
    IS_LISTBOX           CHAR(1),
    IS_LISTBOXITEM       CHAR(1)      not null,
    IS_CMDITEM           CHAR(1)      not null,
    IS_BUTTONGROUP       CHAR(1)      not null,
    IS_TOOLBAR           CHAR(1)      not null,
    IS_CONTROL_VISIBLE   CHAR(1),
    FK_PAT_WINDOWID      DECIMAL(10),
    PROMPT_GR            VARCHAR(60)
);

create unique index PATWCI1
    on PAT_WINCONTROL (FK_PAT_WINDOWID);

