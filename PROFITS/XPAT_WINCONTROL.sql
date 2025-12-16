create table XPAT_WINCONTROL
(
    UID                  DECIMAL(10) not null
        constraint PATXWCPK
            primary key,
    UID_OF_OWNER         DECIMAL(10),
    WINCONTROL_ID        INTEGER,
    GEN_SEQ_NBR          INTEGER,
    OLD_UID              DECIMAL(10),
    OLD_UID_OF_OWNER     DECIMAL(10),
    XLEFT                INTEGER,
    XRIGHT               INTEGER,
    YBOTTOM              INTEGER,
    YTOP                 INTEGER,
    NAME0                CHAR(32)    not null,
    PROMPT               VARCHAR(60),
    HAS_INPUT            CHAR(1),
    STATUS               CHAR(1),
    LAST_CHANGED         TIMESTAMP(6),
    WINCONTROL_ID_SOURCE CHAR(1),
    GEN_WINCONTROL_TYPE  INTEGER     not null,
    AU_WINCONTROL_TYPE   CHAR(1)     not null,
    IS_BTNGROCCURENCE    CHAR(1),
    IS_GROUPITEM         CHAR(1),
    IS_WINFIELD          CHAR(1),
    IS_LISTBOX           CHAR(1),
    IS_LISTBOXITEM       CHAR(1),
    IS_CMDITEM           CHAR(1),
    IS_BUTTONGROUP       CHAR(1),
    IS_TOOLBAR           CHAR(1),
    IS_CONTROL_VISIBLE   CHAR(1),
    PROMPT_GR            CHAR(60),
    FK_XPAT_WINDOWID     DECIMAL(10),
    FK_PAT_WINCONTRUID0  DECIMAL(10)
);

comment on column XPAT_WINCONTROL.UID is 'UNIQUE PER DB ID';

comment on column XPAT_WINCONTROL.WINCONTROL_ID is 'It is  ID of this control. For the majority of the inpit fiedls it is the unique one, assigned by version control. For the rest of the fields it is  the control id assigned by CA GEN.';

comment on column XPAT_WINCONTROL.GEN_SEQ_NBR is 'Sequential number of field, in case it resides in a containter';

comment on column XPAT_WINCONTROL.HAS_INPUT is 'This flag is used as a filter when examining the list of wincontrols.';

comment on column XPAT_WINCONTROL.STATUS is 'The status of the wincontrol fieldtemporal statuses are expressed by letters, the static status is expessed by number';

comment on column XPAT_WINCONTROL.LAST_CHANGED is 'The timestamp indicating when the Wincontrol had been changed via PROFITS Version control procedure.';

comment on column XPAT_WINCONTROL.WINCONTROL_ID_SOURCE is 'Indicate where from the value of the WINCONTROL_ID had beem taken.';

comment on column XPAT_WINCONTROL.AU_WINCONTROL_TYPE is 'COLUMN_TITLE ="_"NULL         =" "PUSHBUTTON = "b"CHECKBOX_BUTTON = "c"DROPDOWN_COMBOBOX = "d"EDIT_FIELD = "e"IMAGE_BUTTON = "i"LISTBOX = "l"MENU_ITEM = "m"BUTTON_WITHOUT_CONTROL_ID (msgwatch) = "n"DROPDOWN_SELECTION_VALUES="o"RADIO_BUTTON = "r"STATICTEX';

comment on column XPAT_WINCONTROL.PROMPT_GR is 'The prompt from encyclopedia, before translation';

create unique index PATXWCI1
    on XPAT_WINCONTROL (FK_XPAT_WINDOWID);

create unique index PATXWCI2
    on XPAT_WINCONTROL (FK_PAT_WINCONTRUID0);

