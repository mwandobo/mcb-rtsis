create table CIE_AGREEMENTS
(
    FK_PROFILE_ID     SMALLINT    not null,
    FK_AGREEMENT_NO   DECIMAL(10) not null,
    CONTROL_ALGORITHM CHAR(1),
    PROMPT_4          CHAR(30),
    IMAGE             CHAR(30),
    PROMPT_3          CHAR(30),
    PROMPT_2          CHAR(30),
    PROMPT_1          CHAR(30),
    GUI_TEXT          CHAR(30),
    constraint IXU_DEF_039
        primary key (FK_PROFILE_ID, FK_AGREEMENT_NO)
);

