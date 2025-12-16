create table VOC_HEADER
(
    VOC_TYPE    CHAR(5),
    DESCRIPTION CHAR(40)
);

create unique index IXP_VOC_000
    on VOC_HEADER (VOC_TYPE);

