create table CPKLASS
(
    KUERZEL     CHAR(10)   not null,
    INSTITUTSNR VARCHAR(4) not null,
    HISTVON     CHAR(17)   not null,
    HISTBIS     CHAR(17)   not null,
    INST_LFD_NR CHAR(4)    not null,
    RISIKOSTUFE DECIMAL(10),
    constraint PK_DUMMY3
        primary key (INSTITUTSNR, KUERZEL)
);

