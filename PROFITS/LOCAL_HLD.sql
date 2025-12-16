create table LOCAL_HLD
(
    DATE_ID     DATE,
    FK_UNITCODE INTEGER,
    DESCRIPTION VARCHAR(40)
);

create unique index IXU_LOC_000
    on LOCAL_HLD (DATE_ID, FK_UNITCODE);

