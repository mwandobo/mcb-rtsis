create table WFS_DETAIL_FLOW
(
    WF_DETAIL_FLOW_STS CHAR(1),
    FK_FLOW_HEADER     DECIMAL(10) not null,
    FK_FLOW_ID         INTEGER     not null,
    FK_DTL_HEADER      DECIMAL(10) not null,
    FK_DTL_ID          DECIMAL(10) not null,
    constraint PK_WF_DETAIL_FLOW
        primary key (FK_FLOW_HEADER, FK_FLOW_ID, FK_DTL_HEADER, FK_DTL_ID)
);

