create table W_EOM_REALTY
(
    EOM_DATE                     DATE,
    REALTY_KEY                   DECIMAL(10) not null,
    MUNICIPALITY                 VARCHAR(40),
    REAL_ESTATE_DESC             CHAR(40),
    LAST_APPRSAL_EVALUATION_DATE DATE,
    LAND_REGISTRY_NUMBER         VARCHAR(50),
    CITY                         VARCHAR(50),
    REGION                       VARCHAR(60),
    COMMERCIAL_VALUE             DECIMAL(15, 2)
);

create unique index PK_W_EOM_REALTY
    on W_EOM_REALTY (EOM_DATE, REALTY_KEY);

CREATE PROCEDURE W_EOM_REALTY ( )
  SPECIFIC SQL160620112636877
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_realty
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
MERGE INTO w_eom_realty a
USING      (SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
                  ,id realty_key
                  ,TRIM (municipality) municipality
                  ,real_estate_desc
                  ,last_evaluation_dt last_apprsal_evaluation_date
                  ,land_regist_id land_registry_number
                  ,city
                  ,region
                  ,commercial_val_amn commercial_value
            FROM   real_estate
                   LEFT JOIN
                   (SELECT   fk_real_estateid
                            ,MAX (evaluation_dt) last_evaluation_dt
                    FROM     real_estate_apprsl
                    GROUP BY fk_real_estateid)
                      ON real_estate.id = fk_real_estateid) b
ON         (a.eom_date = b.eom_date AND a.realty_key = b.realty_key)
WHEN NOT MATCHED
THEN
   INSERT     (
                 eom_date
                ,realty_key
                ,municipality
                ,real_estate_desc
                ,last_apprsal_evaluation_date
                ,land_registry_number
                ,city
                ,region
                ,commercial_value)
   VALUES     (
                 b.eom_date
                ,b.realty_key
                ,b.municipality
                ,b.real_estate_desc
                ,b.last_apprsal_evaluation_date
                ,b.land_registry_number
                ,b.city
                ,b.region
                ,b.commercial_value);
END;

