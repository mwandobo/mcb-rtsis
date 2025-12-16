create table SCANNED_IMAGE_PART
(
    IMAGE_PART_NUMBER INTEGER     not null,
    IMAGE_ID          DECIMAL(12) not null,
    IMAGE_PART_DATA   VARCHAR(4000),
    constraint IXU_SCA_000
        primary key (IMAGE_PART_NUMBER, IMAGE_ID)
);

