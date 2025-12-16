create table EOM_CUST_ADDRESS
(
    FK_CUSTOMERCUST_ID INTEGER,
    SERIAL_NUM         SMALLINT,
    FKGD_HAS_COUNTRY   INTEGER,
    FKGD_HAS_AS_DISTRI INTEGER,
    TMSTAMP            DATE,
    COMMUNICATION_ADDR CHAR(1),
    PTS_IND            CHAR(1),
    ADDRESS_TYPE       CHAR(1),
    ENTRY_STATUS       CHAR(1),
    LATIN_IND          CHAR(1),
    FKGH_HAS_COUNTRY   CHAR(5),
    SEGM_FLAGS         CHAR(5),
    FKGH_HAS_AS_DISTRI CHAR(5),
    MAIL_BOX           CHAR(5),
    ZIP_CODE           CHAR(10),
    FAX_NO             CHAR(15),
    TELEPHONE          CHAR(15),
    CITY               CHAR(30),
    REGION             VARCHAR(20),
    ADDRESS_1          VARCHAR(40),
    ADDRESS_2          VARCHAR(40),
    ENTRY_COMMENTS     VARCHAR(250),
    EOM_DATE           DATE,
    ADDRESS_TYPE_DESC  CHAR(20)
);

create unique index EOM_CUST_ADDRESS_PK
    on EOM_CUST_ADDRESS (EOM_DATE, FK_CUSTOMERCUST_ID, SERIAL_NUM);

CREATE PROCEDURE EOM_CUST_ADDRESS ( )
  SPECIFIC SQL160620112636471
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_cust_address
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_cust_address (
               fk_customercust_id
              ,serial_num
              ,fkgd_has_country
              ,fkgd_has_as_distri
              ,tmstamp
              ,communication_addr
              ,pts_ind
              ,address_type
              ,entry_status
              ,latin_ind
              ,fkgh_has_country
              ,segm_flags
              ,fkgh_has_as_distri
              ,mail_box
              ,zip_code
              ,fax_no
              ,telephone
              ,city
              ,region
              ,address_1
              ,address_2
              ,entry_comments
              ,eom_date
              ,address_type_desc)
   SELECT fk_customercust_id
         ,serial_num
        ,fkgd_has_country
         ,fkgd_has_as_distri
         ,tmstamp
         ,communication_addr
         ,pts_ind
         ,address_type
         ,entry_status
         ,latin_ind
         ,fkgh_has_country
         ,segm_flags
         ,fkgh_has_as_distri
         ,mail_box
         ,zip_code
         ,fax_no
         ,telephone
         ,city
         ,region
         ,address_1
         ,address_2
         ,entry_comments
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,CASE
             WHEN cust_address.address_type = '1' THEN 'Communication'
             WHEN cust_address.address_type = '2' THEN 'Permanent'
             WHEN cust_address.address_type = '3' THEN 'Temporary'
             WHEN cust_address.address_type = '4' THEN 'Work'
             WHEN cust_address.address_type = '5' THEN 'Other'
          END
             address_type_desc
   FROM   cust_address
   WHERE  entry_status = '1';
END;

