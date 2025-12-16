create table TREEVIEW_PROFILES
(
    TREEVIEW_ID INTEGER not null,
    SN          INTEGER not null,
    PROFILE_ID  CHAR(8),
    TMSTAMP     TIMESTAMP(6),
    constraint PK_TVIEW_3
        primary key (SN, TREEVIEW_ID)
);

