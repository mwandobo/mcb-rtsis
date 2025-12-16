create table EOM_REAL_ESTATE
(
    EOM_DATE                   DATE        not null,
    FK_CUSTOMERCUST_ID         INTEGER     not null,
    PROPERTYID                 DECIMAL(10) not null,
    PROPERTYTYPE               DECIMAL(15, 2),
    YEAROFCOMPLETION           SMALLINT,
    FLOORNUMBER                DECIMAL(15, 2),
    LANDAREA                   DECIMAL(10),
    MAINSPACEAREA              DECIMAL(15, 2),
    LANDASSESSEDVALUE          DECIMAL(15, 2),
    PROPERTYTOTALASSESSEDVALUE DECIMAL(15, 2),
    TOTALCONSTRUCTIONCOST      DECIMAL(15, 2),
    TOTALADMINISTRATIVEVALUE   DECIMAL(15, 2),
    APPROVEDLOANAMOUNT         DECIMAL(15, 2),
    TOTALAMOUNTDISBURSED       DECIMAL(15, 2),
    DATEOFVALUATION            DATE,
    INSERTION_DT               DATE,
    MODIFICATION_DT            DATE,
    DATEOFFIRSTDISBURSEMENT    DATE,
    SENDERID                   VARCHAR(3),
    REFERENCEPERIOD            VARCHAR(7),
    YEAROFPERMIT               VARCHAR(10),
    POSTCODE                   VARCHAR(10),
    MUNICIPALITY               CHAR(40),
    DISTRICT                   CHAR(60),
    SENDERNAME                 VARCHAR(20),
    PREFECTURE                 CHAR(50),
    LOANTYPE                   VARCHAR(80),
    STREET                     VARCHAR(92),
    FK_GH_ADDDI                CHAR(5),
    FK_GD_ADDDI                INTEGER,
    FK_GH_CAT                  CHAR(5),
    FK_GD_CAT                  INTEGER,
    FK_GH_CAT_BANK             CHAR(5),
    FK_GD_CAT_BANK             INTEGER,
    FK_GH_COUNTRY              CHAR(5),
    FK_GD_COUNTRY              INTEGER,
    FK_GH_DESCR                CHAR(5),
    FK_GD_DESCR                INTEGER,
    FK_GH_FLOOR                CHAR(5),
    FK_GD_FLOOR                INTEGER,
    FK_GH_INS_COMP             CHAR(5),
    FK_GD_INS_COMP             INTEGER,
    FK_GH_INS_KIND             CHAR(5),
    FK_GD_INS_KIND             INTEGER,
    REGION                     CHAR(60),
    LAND_REGIST_ID             CHAR(50),
    RETYP                      VARCHAR(40),
    EVALUATORS                 VARCHAR(500),
    constraint IXU_EOM_008
        primary key (EOM_DATE, FK_CUSTOMERCUST_ID, PROPERTYID)
);

CREATE PROCEDURE EOM_REAL_ESTATE ( )
  SPECIFIC SQL160620112636266
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_real_estate
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_real_estate (
               eom_date
              ,fk_customercust_id
              ,propertyid
              ,senderid
              ,sendername
              ,referenceperiod
              ,street
              ,postcode
              ,municipality
              ,district
              ,prefecture
              ,dateofvaluation
              ,yearofpermit
              ,yearofcompletion
              ,totalconstructioncost
              ,totaladministrativevalue
              ,insertion_dt
              ,modification_dt
              ,dateoffirstdisbursement
              ,approvedloanamount
              ,totalamountdisbursed
              ,fk_gh_adddi
              ,fk_gd_adddi
              ,fk_gh_cat
              ,fk_gd_cat
              ,fk_gh_cat_bank
              ,fk_gd_cat_bank
              ,fk_gh_country
              ,fk_gd_country
              ,retyp
              ,fk_gh_floor
              ,fk_gd_floor
              ,fk_gh_ins_comp
              ,fk_gd_ins_comp
              ,fk_gh_ins_kind
              ,fk_gd_ins_kind
              ,region
              ,land_regist_id)
   WITH apprsl
        AS (SELECT   fk_real_estateid
                    ,MAX (evaluation_dt) evaluation_dt
                    ,SUM (commerc_value) property_account_val
            FROM     real_estate_apprsl
            WHERE    entry_status = 1
            GROUP BY fk_real_estateid)
   SELECT (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,real_estate_cust.fk_customercust_id
         ,id AS propertyid
         ,TRIM (bp.bank_code) AS senderid
         ,SUBSTR ( (bp.bank_name), 1, 20) AS sendername
         ,TO_CHAR (
             ADD_MONTHS ( (SELECT scheduled_date FROM bank_parameters), -1)
            ,'MM/YYYY')
             AS referenceperiod
         ,TRIM (re.address) || ' ' || TRIM (address_num) AS street
         ,re.zip_code AS postcode
         ,municipality AS municipality
         ,re.region AS district
         ,city AS prefecture
         ,evaluation_dt AS dateofvaluation
         ,'00/00/0001' AS yearofpermit
         ,construction_year AS yearofcompletion
         ,0 AS totalconstructioncost
         ,objective_amn AS totaladministrativevalue
         ,re.insertion_dt
         ,re.modification_dt
         ,TO_DATE ('01/01/0001', 'DD/MM/YYYY') AS dateoffirstdisbursement
         ,0 AS approvedloanamount
         ,0 AS totalamountdisbursed
         ,fk_gh_adddi
         ,fk_gd_adddi
         ,fk_gh_cat
         ,fk_gd_cat
         ,fk_gh_cat_bank
         ,fk_gd_cat_bank
         ,fk_gh_country
         ,fk_gd_country
         ,retyp.description retyp
         ,fk_gh_floor
         ,fk_gd_floor
         ,fk_gh_ins_comp
         ,fk_gd_ins_comp
         ,fk_gh_ins_kind
         ,fk_gd_ins_kind
         ,re.region
         ,land_regist_id
   FROM   bank_parameters bp
          INNER JOIN real_estate re ON (1 = 1)
          LEFT JOIN generic_detail retyp
             ON (    re.fk_gh_descr = retyp.fk_generic_headpar
                 AND re.fk_gd_descr = retyp.serial_num)
          LEFT JOIN real_estate_cust
             ON (re.id = real_estate_cust.fk_real_estateid)
          LEFT JOIN apprsl ON (re.id = apprsl.fk_real_estateid)
   WHERE  re.entry_status = '1' AND real_estate_cust.entry_status = '1';
END;

