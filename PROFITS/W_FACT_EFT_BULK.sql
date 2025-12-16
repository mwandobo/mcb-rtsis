CREATE VIEW W_FACT_EFT_BULK
(
   DATA_ROW_SN,
   FILE_NAME,
   TRX_DATE,
   TRX_UNIT,
   TRX_USR,
   CR_USR_SN,
   SALARY_FLAG,
   PROCESS_STATUS_FLAG,
   ORDER_INSTRUMENT_IND,
   PAYERS_ACCOUNT_NO,
   PAYERS_CUST_ID,
   CUSTOMER_NAME,
   BENEFICIARY_NAME,
   CUSTOMER_ACCOUNT_NO,
   AMOUNT,
   REJECTION_REASON,
   BENEF_FULLNAME,
   BENEF_BIC_ADDRESS,
   BENEF_BANK_NAME,
   CURRENCY_CODE,
   CHARGE_AMOUNT,
   TAX_AMOUNT,
   ORDER_CODE
)
   AS
   WITH fs
        AS (  SELECT t.trx_unit,
                     t.trx_date,
                     t.trx_usr,
                     t.trx_sn,
                     MAX (DECODE (entry_ser_num, 2, entry_amount))
                        charge_amount,
                     MAX (DECODE (entry_ser_num, 3, entry_amount)) tax_amount
                FROM fst_demand_extrait t
               WHERE id_transact = 3181
            GROUP BY t.trx_unit,
                     t.trx_date,
                     t.trx_usr,
                     t.trx_sn),
        pa
        AS (  SELECT p.account_number account_number,
                     CASE
                        WHEN P.PRODUCT_ID IN (SELECT J.FKGD_HAS_A_PRIMARY
                                                FROM par_relation_detai j
                                               WHERE J.FK_PAR_RELATIONCOD =
                                                        'ONEACC')
                        THEN
                           3
                        ELSE
                           MAX (prft_system)
                     END
                        prft_system,
                     account_cd,
                     p.cust_id
                FROM profits_account p
                     LEFT JOIN customer c ON c.cust_id = p.cust_id
               WHERE prft_system IN (3, 4)
            GROUP BY p.account_number,
                     P.PRODUCT_ID,
                     p.account_cd,
                     p.cust_id)
   SELECT b.data_row_sn,
          b.file_name,
          b.trx_date,
          b.trx_unit,
          b.trx_usr,
          b.cr_usr_sn,
          CASE
             WHEN b.template_id IN (9,
                                    18,
                                    19,
                                    21)
             THEN
                'Salary'
             ELSE
                'Not Salary'
          END
             salary_flag,
          DECODE (b.process_status, '1', 'Successful', 'Unsuccessful')
             process_status_flag,
          CASE TRIM (b.order_instrument)
             WHEN 'STP' THEN 'EFT'
             ELSE DECODE (b.loan_justific, 0, 'Deposit', 'Loan' )
          END
             order_instrument_ind,
             TRIM (db_prft_account)
          || NVL2 (pa2.account_cd, '-' || pa2.account_cd, NULL)
             payers_account_no,
          pa2.cust_id payers_cust_id,
             TRIM (c.first_name)
          || ' '
          || TRIM (c.middle_name)
          || ' '
          || TRIM (c.surname)
             customer_name,
          b.employees_name beneficiary_name,
             TRIM (cr_prft_account)
          || NVL2 (pa1.account_cd, '-' || pa1.account_cd, NULL)
             customer_account_no,
          b.amount amount,
          b.rejection_reason,
          b.benef_fullname,
          b.benef_bic_address,
          (SELECT SUBSTR (
                     bank_name,
                     1,
                     DECODE (INSTR (bank_name, ' - '),
                             0, LENGTH (bank_name),
                             INSTR (bank_name, ' - ')))
                     bank_name
             FROM frt_bank
            WHERE     SUBSTR (sort_code, 1, 2) = b.bank_code
                  AND SUBSTR (sort_code, 4, 2) = b.branch_code)
             benef_bank_name,
          cu.short_descr currency_code,
          charge_amount,
          tax_amount,
          b.order_code
     FROM blk_file_detail b
          LEFT JOIN pa pa1 ON b.cr_prft_account = pa1.account_number
          LEFT JOIN customer c ON c.cust_id = pa1.cust_id
          LEFT JOIN pa pa2 ON b.db_prft_account = pa2.account_number
          LEFT JOIN currency cu ON cu.id_currency = b.currency_id
          LEFT JOIN fs
             ON     b.trx_unit = fs.trx_unit
                AND b.trx_date = fs.trx_date
                AND b.trx_usr = fs.trx_usr
                AND b.cr_usr_sn = fs.trx_sn
                AND b.cr_usr_sn > 0
    WHERE b.comments !=
             (SELECT string_50
                FROM profits_param
               WHERE     transact_id = 2180
                     AND param_name = 'BULK_EFT_HD_CHG_COMM'
                     AND active_flag = 'Y');

