create table GLG_ZERO_BALANCE_PARAM
(
    REMARKS_INDICATOR CHAR(1)     not null,
    COMMENT_REMARKS   VARCHAR(80) not null,
    JUSTIFY_ID        CHAR(4)     not null,
    DOC_ID            CHAR(4)     not null,
    DOC_SER           CHAR(2)     not null,
    JUSTIFIC_IND      CHAR(1),
    ZB_TB_IND         CHAR(1),
    constraint PK_ZERO_BAL
        primary key (DOC_SER, DOC_ID, JUSTIFY_ID, COMMENT_REMARKS, REMARKS_INDICATOR)
);

comment on column GLG_ZERO_BALANCE_PARAM.COMMENT_REMARKS is 'it is the remarks insert from the Glg_temp_trn remarks attribute after the transactions have been accounted for';

comment on column GLG_ZERO_BALANCE_PARAM.JUSTIFY_ID is 'It is the number assigned to the justification.It isuniquely defined in the information system.*****************Comments****************** JU TrxJustification Code';

comment on column GLG_ZERO_BALANCE_PARAM.DOC_ID is 'DT Document Type Code';

comment on column GLG_ZERO_BALANCE_PARAM.DOC_SER is 'DT Document Series';

