create table SCORING_ACCOUNTS
(
    FK_SCORING_SN DECIMAL(10) not null,
    ACCOUNT_UNIT  INTEGER     not null,
    ACC_TYPE      SMALLINT    not null,
    ACC_SN        INTEGER     not null,
    constraint PIXSCACC
        primary key (FK_SCORING_SN, ACCOUNT_UNIT, ACC_TYPE, ACC_SN)
);

