create table TREEVIEW_NODES
(
    TREEVIEW_ID INTEGER      not null,
    NODE_ID     CHAR(7)      not null,
    NODE_PARENT CHAR(7),
    NODE_TEXT   CHAR(60),
    NODE_ORDER  INTEGER,
    NODE_SYSTEM INTEGER,
    NODE_LEVEL  SMALLINT,
    TMSTAMP     TIMESTAMP(6) not null,
    constraint PK_TVIEW
        primary key (NODE_ID, TREEVIEW_ID)
);

