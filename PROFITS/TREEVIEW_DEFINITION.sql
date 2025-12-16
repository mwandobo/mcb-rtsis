create table TREEVIEW_DEFINITION
(
    TREEVIEW_ID INTEGER not null
        constraint PK_TVIEW_2
            primary key,
    DESCRIPTION CHAR(80),
    TMSTAMP     TIMESTAMP(6)
);

