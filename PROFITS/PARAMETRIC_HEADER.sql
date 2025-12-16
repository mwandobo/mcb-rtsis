create table PARAMETRIC_HEADER
(
    ID               INTEGER,
    FLAG_1           CHAR(1),
    FLAG_9           CHAR(1),
    FLAG_8           CHAR(1),
    FLAG_7           CHAR(1),
    FLAG_6           CHAR(1),
    FLAG_5           CHAR(1),
    FLAG_4           CHAR(1),
    FLAG_3           CHAR(1),
    FLAG_2           CHAR(1),
    FLAG_13          CHAR(10),
    FLAG_12          CHAR(10),
    FLAG_11          CHAR(10),
    FLAG_10          CHAR(10),
    FLAG_15          CHAR(10),
    FLAG_14          CHAR(10),
    RULE_DESCRIPTION VARCHAR(2000)
);

create unique index IXU_PAR_006
    on PARAMETRIC_HEADER (ID);

