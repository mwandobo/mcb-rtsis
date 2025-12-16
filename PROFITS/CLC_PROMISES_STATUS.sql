create table CLC_PROMISES_STATUS
(
    PAY_PROMISE_ID          INTEGER not null,
    TRX_DATE                DATE    not null,
    CLC_FIRST_STS_PREV      CHAR(2),
    FK_CLC_SECOND_STS_PREV  INTEGER,
    FK_CLC_JUST_REASON_PREV INTEGER,
    CLC_FIRST_STS_NXT       CHAR(2),
    FK_CLC_SECOND_STS_NXT   INTEGER,
    FK_CLC_JUST_REASON_NXT  INTEGER,
    constraint PK_CLC_STS
        primary key (PAY_PROMISE_ID, TRX_DATE)
);

