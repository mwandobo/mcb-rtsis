create table W_STG_IMAGE_CUSTOMER
(
    IMAGE_ID   DECIMAL(12) not null,
    CUST_ID    DECIMAL(7)  not null,
    IMAGE_TYPE VARCHAR(20),
    constraint PK_W_STG_IMAGE_CUSTOMER
        primary key (IMAGE_ID, CUST_ID)
);

CREATE PROCEDURE W_STG_IMAGE_CUSTOMER ( )
  SPECIFIC SQL160620112633045
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
 INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_stg_image_customer;
INSERT INTO w_stg_image_customer (image_id, cust_id, image_type)
   SELECT s.image_id, c.cust_id, s.image_type
   FROM   scanned_image s
          INNER JOIN customer c
             ON     TRIM (object_id) = TRIM (TO_CHAR (c.cust_id))
                AND c.cust_type = '1';
END;

