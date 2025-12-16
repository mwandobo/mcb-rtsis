create table EOM_COLLAT_PLEDGE
(
    EOM_DATE               DATE,
    INTERNAL_SN            DECIMAL(15, 2),
    RECORD_TYPE            VARCHAR(2),
    COLLAT_DTL_INTERNAL_SN DECIMAL(15, 2),
    RECORD_DESCR           CHAR(40),
    REAL_ESTATE_ID         DECIMAL(10),
    PROFITS_ACCOUNT_1      CHAR(40),
    ACCOUNT_CD_1           SMALLINT,
    PRFT_SYSTEM_1          SMALLINT,
    PROFITS_ACCOUNT_2      CHAR(40),
    ACCOUNT_CD_2           SMALLINT,
    PRFT_SYSTEM_2          SMALLINT,
    CUST_ID_1              INTEGER,
    C_DIGIT_1              SMALLINT,
    CUST_ID_2              INTEGER,
    C_DIGIT_2              SMALLINT,
    CURRENCY_ID            INTEGER,
    COLLABORATION_BANK     INTEGER,
    BOND_CODE              CHAR(20),
    DATE_1                 DATE,
    DATE_2                 DATE,
    DATE_3                 DATE,
    DATE_4                 DATE,
    DATE_5                 DATE,
    DATE_6                 DATE,
    DATE_7                 DATE,
    DATE_8                 DATE,
    DATE_9                 DATE,
    DATE_10                DATE,
    AMOUNT_1               DECIMAL(15, 2),
    AMOUNT_2               DECIMAL(15, 2),
    AMOUNT_3               DECIMAL(15, 2),
    AMOUNT_4               DECIMAL(15, 2),
    AMOUNT_5               DECIMAL(15, 2),
    AMOUNT_6               DECIMAL(15, 2),
    AMOUNT_7               DECIMAL(15, 2),
    AMOUNT_8               DECIMAL(15, 2),
    AMOUNT_9               DECIMAL(15, 2),
    AMOUNT_10              DECIMAL(15, 2),
    NUMBER_1               DECIMAL(10),
    NUMBER_2               DECIMAL(10),
    NUMBER_3               DECIMAL(10),
    NUMBER_4               DECIMAL(11),
    NUMBER_5               DECIMAL(10),
    NUMBER_6               DECIMAL(10),
    NUMBER_7               DECIMAL(10),
    NUMBER_8               DECIMAL(10),
    NUMBER_9               DECIMAL(10),
    NUMBER_10              DECIMAL(10),
    DESCR_1                CHAR(40),
    DESCR_2                CHAR(40),
    DESCR_3                CHAR(40),
    DESCR_4                CHAR(40),
    DESCR_5                CHAR(40),
    DESCR_6                CHAR(40),
    DESCR_7                CHAR(40),
    DESCR_8                CHAR(40),
    DESCR_9                CHAR(40),
    DESCR_10               CHAR(40),
    DESCR_11               CHAR(40),
    DESCR_12               CHAR(40),
    DESCR_13               CHAR(40),
    DESCR_14               CHAR(40),
    DESCR_15               CHAR(40),
    DESCR_16               CHAR(40),
    DESCR_17               CHAR(40),
    DESCR_18               CHAR(40),
    DESCR_19               CHAR(40),
    DESCR_20               CHAR(40),
    FLAG_1                 CHAR(1),
    FLAG_2                 CHAR(1),
    FLAG_3                 CHAR(1),
    FLAG_4                 CHAR(1),
    FLAG_5                 CHAR(1),
    FLAG_6                 CHAR(1),
    FLAG_7                 CHAR(1),
    FLAG_8                 CHAR(1),
    FLAG_9                 CHAR(1),
    FLAG_10                CHAR(1),
    GD_PAR_TYPE_1          CHAR(5),
    GD_SERIAL_NUM_1        INTEGER,
    GD_PAR_TYPE_2          CHAR(5),
    GD_SERIAL_NUM_2        INTEGER,
    GD_PAR_TYPE_3          CHAR(5),
    GD_SERIAL_NUM_3        INTEGER,
    GD_PAR_TYPE_4          CHAR(5),
    GD_SERIAL_NUM_4        INTEGER,
    GD_PAR_TYPE_5          CHAR(5),
    GD_SERIAL_NUM_5        INTEGER,
    GD_PAR_TYPE_6          CHAR(5),
    GD_SERIAL_NUM_6        INTEGER,
    LARGE_DESCR_1          CHAR(254),
    LAGRE_DESCR_2          CHAR(254),
    LARGE_DESCR_3          CHAR(254),
    TRX_UNIT               INTEGER,
    INSERTION_USR          CHAR(8),
    INSERTION_DT           DATE,
    MODIFICATION_DT        DATE,
    MODIFICATION_USR       CHAR(8),
    ENTRY_STATUS           CHAR(1),
    COLLATERAL_USAGE       CHAR(1),
    USED_COLLAT_SN         DECIMAL(10),
    USED_UNIT              INTEGER,
    USED_COLLAT_TYPE       INTEGER,
    AMOUNT_18_4_1          DECIMAL(18, 4),
    AMOUNT_18_4_2          DECIMAL(18, 4),
    AMOUNT_18_4_3          DECIMAL(18, 4),
    COMMENTS               VARCHAR(255),
    COUNTRY_COURT          CHAR(40),
    MORTGAGE_IND           CHAR(1),
    REQUIRED_IND           CHAR(1),
    TAG_SET_CODE           CHAR(20),
    COLLAT_DTL_COMMENTS    CHAR(254),
    FK_GD_HAS_AS_CARRI     INTEGER,
    FK_GD_HAS_LAND_REG     INTEGER,
    FK_GD_HAS_SERIAL       INTEGER,
    FK_GH_HAS_AS_CARRI     CHAR(5),
    FK_GH_HAS_LAND_REG     CHAR(5),
    FK_GH_HAS_SERIAL       CHAR(5),
    LAWYER                 VARCHAR(40),
    MAIN_CONNECT_IND       CHAR(1),
    PRENOTATION_AMN        DECIMAL(15, 2),
    REMOVAL_DATE           DATE,
    REMOVAL_IND            CHAR(1),
    SHEET                  VARCHAR(40),
    VOLUME                 VARCHAR(40)
);

create unique index EOM_COLLAT_PLEDGE_PK
    on EOM_COLLAT_PLEDGE (EOM_DATE, INTERNAL_SN, RECORD_TYPE, COLLAT_DTL_INTERNAL_SN);

CREATE PROCEDURE EOM_COLLAT_PLEDGE ( )
  SPECIFIC SQL160620112634059
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_collat_pledge
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_collat_pledge (
               eom_date
              ,internal_sn
              ,record_type
              ,collat_dtl_internal_sn
              ,record_descr
              ,real_estate_id
              ,profits_account_1
              ,account_cd_1
              ,prft_system_1
              ,profits_account_2
              ,account_cd_2
              ,prft_system_2
              ,cust_id_1
              ,c_digit_1
              ,cust_id_2
              ,c_digit_2
              ,currency_id
              ,collaboration_bank
              ,bond_code
              ,date_1
              ,date_2
              ,date_3
              ,date_4
              ,date_5
              ,date_6
              ,date_7
              ,date_8
              ,date_9
              ,date_10
              ,amount_1
              ,amount_2
              ,amount_3
              ,amount_4
              ,amount_5
              ,amount_6
              ,amount_7
              ,amount_8
              ,amount_9
              ,amount_10
              ,number_1
              ,number_2
              ,number_3
              ,number_4
              ,number_5
              ,number_6
              ,number_7
              ,number_8
              ,number_9
              ,number_10
              ,descr_1
              ,descr_2
              ,descr_3
              ,descr_4
              ,descr_5
              ,descr_6
              ,descr_7
              ,descr_8
              ,descr_9
              ,descr_10
              ,descr_11
              ,descr_12
              ,descr_13
              ,descr_14
              ,descr_15
              ,descr_16
              ,descr_17
              ,descr_18
              ,descr_19
              ,descr_20
              ,flag_1
              ,flag_2
              ,flag_3
              ,flag_4
              ,flag_5
              ,flag_6
              ,flag_7
              ,flag_8
              ,flag_9
              ,flag_10
              ,gd_par_type_1
              ,gd_serial_num_1
              ,gd_par_type_2
              ,gd_serial_num_2
              ,gd_par_type_3
              ,gd_serial_num_3
              ,gd_par_type_4
              ,gd_serial_num_4
              ,gd_par_type_5
              ,gd_serial_num_5
              ,gd_par_type_6
              ,gd_serial_num_6
              ,large_descr_1
              ,lagre_descr_2
              ,large_descr_3
              ,trx_unit
              ,insertion_usr
              ,insertion_dt
              ,modification_dt
              ,modification_usr
              ,entry_status
              ,collateral_usage
              ,used_collat_sn
              ,used_unit
              ,used_collat_type
              ,amount_18_4_1
              ,amount_18_4_2
              ,amount_18_4_3
              ,comments
              ,country_court
              ,mortgage_ind
              ,required_ind
              ,tag_set_code
              ,collat_dtl_comments
              ,fk_gd_has_as_carri
              ,fk_gd_has_land_reg
              ,fk_gd_has_serial
              ,fk_gh_has_as_carri
              ,fk_gh_has_land_reg
              ,fk_gh_has_serial
              ,lawyer
              ,main_connect_ind
              ,prenotation_amn
              ,removal_date
              ,removal_ind
              ,sheet
              ,volume)
   SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
         ,NVL (ct.internal_sn, 0) AS internal_sn
         ,NVL (ct.record_type, 0) AS record_type
         ,NVL (cd.internal_sn, 0) AS collat_dtl_internal_sn
         ,ct.record_descr
         ,cd.real_estate_id
         ,ct.profits_account_1
         ,ct.account_cd_1
         ,ct.prft_system_1
         ,ct.profits_account_2
         ,ct.account_cd_2
         ,ct.prft_system_2
         ,ct.cust_id_1
         ,ct.c_digit_1
         ,ct.cust_id_2
         ,ct.c_digit_2
         ,ct.currency_id
         ,ct.collaboration_bank
         ,ct.bond_code
         ,ct.date_1
         ,ct.date_2
         ,ct.date_3
         ,ct.date_4
         ,ct.date_5
         ,ct.date_6
         ,ct.date_7
         ,ct.date_8
         ,ct.date_9
         ,ct.date_10
         ,ct.amount_1
         ,ct.amount_2
         ,ct.amount_3
         ,ct.amount_4
         ,ct.amount_5
         ,ct.amount_6
         ,ct.amount_7
         ,ct.amount_8
         ,ct.amount_9
         ,ct.amount_10
         ,ct.number_1
         ,ct.number_2
         ,ct.number_3
         ,ct.number_4
         ,ct.number_5
         ,ct.number_6
         ,ct.number_7
         ,ct.number_8
         ,ct.number_9
         ,ct.number_10
         ,ct.descr_1
         ,ct.descr_2
         ,ct.descr_3
         ,ct.descr_4
         ,ct.descr_5
         ,ct.descr_6
         ,ct.descr_7
         ,ct.descr_8
         ,ct.descr_9
         ,ct.descr_10
         ,ct.descr_11
         ,ct.descr_12
         ,ct.descr_13
         ,ct.descr_14
         ,ct.descr_15
         ,ct.descr_16
         ,ct.descr_17
         ,ct.descr_18
         ,ct.descr_19
         ,ct.descr_20
         ,ct.flag_1
         ,ct.flag_2
         ,ct.flag_3
         ,ct.flag_4
         ,ct.flag_5
         ,ct.flag_6
         ,ct.flag_7
         ,ct.flag_8
         ,ct.flag_9
         ,ct.flag_10
         ,ct.gd_par_type_1
         ,ct.gd_serial_num_1
         ,ct.gd_par_type_2
         ,ct.gd_serial_num_2
         ,ct.gd_par_type_3
         ,ct.gd_serial_num_3
         ,ct.gd_par_type_4
         ,ct.gd_serial_num_4
         ,ct.gd_par_type_5
         ,ct.gd_serial_num_5
         ,ct.gd_par_type_6
         ,ct.gd_serial_num_6
         ,ct.large_descr_1
         ,ct.lagre_descr_2
         ,ct.large_descr_3
         ,ct.trx_unit
         ,ct.insertion_usr
         ,ct.insertion_dt
         ,ct.modification_dt
         ,ct.modification_usr
         ,ct.entry_status
         ,ct.collateral_usage
         ,ct.used_collat_sn
         ,ct.used_unit
         ,ct.used_collat_type
         ,ct.amount_18_4_1
         ,ct.amount_18_4_2
         ,ct.amount_18_4_3
         ,ct.comments
         ,ct.country_court
         ,ct.mortgage_ind
         ,ct.required_ind
         ,ct.tag_set_code
         ,cd.comments collat_dtl_comments
         ,cd.fk_gd_has_as_carri
         ,cd.fk_gd_has_land_reg
         ,cd.fk_gd_has_serial
         ,cd.fk_gh_has_as_carri
         ,cd.fk_gh_has_land_reg
         ,cd.fk_gh_has_serial
         ,cd.lawyer
         ,cd.main_connect_ind
         ,cd.prenotation_amn
         ,cd.removal_date
         ,cd.removal_ind
         ,cd.sheet
         ,cd.volume
   FROM   collateral_table ct
          LEFT JOIN collateral_detail cd
             ON (    ct.record_type = cd.record_type
                 AND ct.internal_sn = cd.ctbl_internal_sn);
END;

