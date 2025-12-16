create table SWIFT_COUNTER
(
    DATE0   DATE,
    COUNTER INTEGER
);

create unique index IXU_SWI_013
    on SWIFT_COUNTER (DATE0, COUNTER);

