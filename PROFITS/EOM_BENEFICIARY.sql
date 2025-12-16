create table EOM_BENEFICIARY
(
    EOM_DATE           DATE,
    FK_CUSTOMERCUST_ID INTEGER,
    FK_DEPOSIT_ACCOACC DECIMAL(11),
    BENEFICIARY_SN     SMALLINT,
    ACCT_KEY           DECIMAL(11)
);

create unique index EOM_BENEFICIARY_PK
    on EOM_BENEFICIARY (FK_DEPOSIT_ACCOACC, FK_CUSTOMERCUST_ID, EOM_DATE);

CREATE PROCEDURE EOM_BENEFICIARY ( )
  SPECIFIC SQL160620112634058
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_beneficiary
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_beneficiary (
               fk_deposit_accoacc
              ,fk_customercust_id
              ,eom_date
              ,beneficiary_sn
              ,acct_key)
   SELECT fk_deposit_accoacc
         ,fk_customercust_id
         , (SELECT scheduled_date FROM bank_parameters) eom_date
         ,beneficiary_sn
         ,account_ser_num acct_key
   FROM   beneficiary
          INNER JOIN profits_account
             ON     beneficiary.fk_deposit_accoacc =
                       profits_account.dep_acc_number
                AND profits_account.prft_system = 3
   WHERE  profits_account.dep_acc_number <> 0;
END;

