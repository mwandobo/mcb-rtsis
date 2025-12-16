create table APPLICATION_ACCOUN
(
    TMSTAMP                    DATE     not null,
    FK_LOAN_ACC_ACC_SN         INTEGER  not null,
    FK_LOAN_ACC_ACCTYP         SMALLINT not null,
    FK_CSC_APP_GH_LDEP         CHAR(5)  not null,
    FK_CSC_APP_GD_LDEP         INTEGER  not null,
    FK_CSC_APP_APP_SN          INTEGER  not null,
    FK_CSC_APP_SC_YEAR         INTEGER  not null,
    FK_LOAN_ACCOUNTFK_UNITCODE INTEGER  not null,
    FK_CSC_APPLICATFK_UNITCODE INTEGER  not null,
    constraint I0000907
        primary key (FK_CSC_APPLICATFK_UNITCODE, FK_LOAN_ACCOUNTFK_UNITCODE, FK_LOAN_ACC_ACC_SN, FK_LOAN_ACC_ACCTYP,
                     FK_CSC_APP_GH_LDEP, FK_CSC_APP_GD_LDEP, FK_CSC_APP_APP_SN, FK_CSC_APP_SC_YEAR)
);

