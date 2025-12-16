create table XPAT_USER_PROFILE
(
    ID                CHAR(10)  not null
        constraint PATXMNPK
            primary key,
    DESCRIPTION       CHAR(32)  not null,
    BANK_NAME         CHAR(10)  not null,
    DB_CONNECT_STRING CHAR(240) not null,
    APP_ARGUMENTS     CHAR(80),
    ACCESS_RIGHTS     SMALLINT,
    REPORT_SAVE_PATH  CHAR(60),
    CLIENT_TYPE       CHAR(1),
    LANGUAGE_CODE     CHAR(2),
    VALUE_OWNERSHIP   CHAR(1)   not null,
    STATUS            CHAR(1)   not null
);

comment on table XPAT_USER_PROFILE is 'This entity defines export import table for pat_user_profile records';

comment on column XPAT_USER_PROFILE.ID is 'Unique id assigned to each manager. The ROOT id is reserved for being used in the PAT_GENVALUE_TYPE and PAT_GENERIC_VALUE of usage "C"- common. This attibute is important for values that are created/used/logged during test execution, i.e. variables that';

comment on column XPAT_USER_PROFILE.DESCRIPTION is 'Descriptive attribute, can hold the data about person which uses the given Profile during the test. This attribute is logged during test running, so it helps to distinguish who executed the test.';

comment on column XPAT_USER_PROFILE.BANK_NAME is 'Descriptive attribure. Can be the BANK for which the tests to be run, or short name of DB/environment''s alias.';

comment on column XPAT_USER_PROFILE.DB_CONNECT_STRING is 'PROFITS DB connect string, This field is used alsp for chef profile look-up.';

comment on column XPAT_USER_PROFILE.APP_ARGUMENTS is 'Defines which arguments are passed to PROFITS executable. (e.g. trancode and branch)';

comment on column XPAT_USER_PROFILE.ACCESS_RIGHTS is 'To be used';

comment on column XPAT_USER_PROFILE.REPORT_SAVE_PATH is 'The path of documents repository to which the execution report document can be copied/saved from local directory (where it was created)';

comment on column XPAT_USER_PROFILE.CLIENT_TYPE is 'Defines which PROFITS Clients (Cooperative, Windows GUI or TUXEDO ) this User is going to run.';

comment on column XPAT_USER_PROFILE.LANGUAGE_CODE is 'For users having Tester, Root and Bank profiles this value defines which language is going to be used during Tests ( They borrow values from correspondent Localisers)For Localisers this attribute shows what language used for values, which they own.Curren';

comment on column XPAT_USER_PROFILE.VALUE_OWNERSHIP is 'Defines which set of pat_genvalue_types and pat_generic_values are allowed to be used and owned by this USER. Corresponds to the Usage attribute of above tables. ROOT is excusitive owner of Common no-language specific values and has set of values of ever';

