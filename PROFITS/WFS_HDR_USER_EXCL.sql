create table WFS_HDR_USER_EXCL
(
    USER_ID      CHAR(8),
    FK_WF_HEADER DECIMAL(10),
    USER_STATUS  CHAR(1)
);

create unique index PK_WFH_UEXCL
    on WFS_HDR_USER_EXCL (USER_ID, FK_WF_HEADER);

