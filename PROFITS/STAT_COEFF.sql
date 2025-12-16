create table STAT_COEFF
(
    PROGRAM_ID  CHAR(5) not null
        constraint IXU_DEP_174
            primary key,
    COEFFICIENT DECIMAL(15)
);

