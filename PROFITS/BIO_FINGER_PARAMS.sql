create table BIO_FINGER_PARAMS
(
    LABEL       VARCHAR(50) not null
        constraint BIO_FINGER_PARAMS_PK
            primary key,
    VALUE       VARCHAR(255),
    DESCRIPTION VARCHAR(255)
);

