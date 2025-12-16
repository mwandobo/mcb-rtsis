create table CUST_SEGMENTATION
(
    CUST_TYPE       CHAR(1)  not null,
    SEGMENT         CHAR(40) not null,
    SEGMENT_CODE    CHAR(2)  not null,
    SUBSEGMENT      CHAR(40) not null,
    SUBSEGMENT_CODE CHAR(2)  not null,
    CALCULATION     CHAR(20) not null,
    AMOUNT_FROM     DECIMAL(15, 2),
    AMOUNT_TO       DECIMAL(15, 2),
    constraint PK_CUST_SEGMENTATION
        primary key (CUST_TYPE, SEGMENT, SEGMENT_CODE, SUBSEGMENT, SUBSEGMENT_CODE, CALCULATION)
);

