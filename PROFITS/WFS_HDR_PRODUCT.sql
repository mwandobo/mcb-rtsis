create table WFS_HDR_PRODUCT
(
    ID_PRODUCT     INTEGER     not null,
    PRODUCT_TYPE   CHAR(1),
    PRODUCT_STATUS CHAR(1),
    FK_WF_HEADER   DECIMAL(10) not null,
    constraint PF_WFS_HDR_PROD
        primary key (FK_WF_HEADER, ID_PRODUCT)
);

