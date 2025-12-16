create table W_FACT_REALTY_APPRAISAL
(
    REALTY_KEY         DECIMAL(10) not null,
    APPRSL_INTERNAL_SN DECIMAL(10) not null,
    EVALUATION_DT      DATE,
    SELL_VALUE         DECIMAL(15, 2)
);

create unique index PK_W_FACT_REALTY_APPRAISAL
    on W_FACT_REALTY_APPRAISAL (REALTY_KEY, EVALUATION_DT, APPRSL_INTERNAL_SN);

CREATE PROCEDURE W_FACT_REALTY_APPRAISAL ( )
  SPECIFIC SQL160620112637080
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_fact_realty_appraisal a
USING      (SELECT fk_real_estateid realty_key
                  ,internal_sn apprsl_internal_sn
                  ,evaluation_dt
                  ,sell_value
            FROM   real_estate_apprsl) b
ON         (    a.realty_key = b.realty_key
            AND a.apprsl_internal_sn = b.apprsl_internal_sn)
WHEN NOT MATCHED
THEN
   INSERT     (
                 realty_key
                ,apprsl_internal_sn
                ,evaluation_dt
                ,sell_value)
   VALUES     (
                 b.realty_key
                ,b.apprsl_internal_sn
                ,b.evaluation_dt
                ,b.sell_value);
END;

