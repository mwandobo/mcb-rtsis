create table GLG_D_TRANSITION
(
    FK_GLG_H_TRANSITRA CHAR(4),
    FROM_ACCOUNT       CHAR(21),
    TIMESTMP           DATE,
    STATUS             CHAR(1),
    SECOND_MID_ACC     CHAR(21),
    IN_ACCOUNT         CHAR(21),
    TO_ACCOUNT         CHAR(21),
    FIRST_MID_ACC      CHAR(21)
);

create unique index IXP_GLG_001
    on GLG_D_TRANSITION (FK_GLG_H_TRANSITRA, FROM_ACCOUNT);

