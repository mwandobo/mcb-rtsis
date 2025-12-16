CREATE VIEW W_FACT_STANDING_ORDER_PAYMENT
(
   UNIT_CODE,
   SO_NUMBER,
   DESCRIPTION,
   EXECUTION_DATE,
   STATUS_IND,
   SO_METHOD_IND,
   ACTUAL_PAYMENT_AMOUNT,
   CURRENCY_CODE,
   FIRST_PAYMENT_DATE,
   NEXT_PAYMENT_DATE,
   EXPIRATION_DATE,
   DEBIT_ACCOUNT_NUMBER,
   DEBIT_ACCOUNT_DESIGNATION,
   DEBIT_ACCOUNT_BENEFICIARIES,
   DEBIT_ACCOUNT_PRODUCT,
   DEBIT_ACCOUNT_TYPE,
   CREDIT_ACCOUNT_NUMBER,
   CREDIT_ACCOUNT_DESIGNATION,
   CREDIT_ACCOUNT_TYPE,
   CREDIT_ACCOUNT_PRODUCT,
   ORIGINATOR_NAME,
   BENEFICIARY_NAME
)
AS
   WITH t
        AS (  SELECT fk_deposit_accoacc,
                     LISTAGG (
                        TRIM (cust.first_name) || ' ' || TRIM (cust.surname),
                        ', ')
                     WITHIN GROUP (ORDER BY be.beneficiary_sn)
                        beneficiaries_name
                FROM beneficiary be
                     LEFT JOIN customer cust
                        ON cust.cust_id = be.fk_customercust_id
            GROUP BY fk_deposit_accoacc)
   SELECT h.unit_code,
          h.tp_so_identifier so_number,
          t.description,
          h.activation_date execution_date,
          DECODE (h.entry_status,
                  '0', 'Pending',
                  '1', 'Successful',
                  '2', 'Cancelled',
                  'Other')
             status_ind,
          DECODE (h.stand_order_method,
                  '0', 'EFT',
                  '1', 'Transfer Between Accounts',
                  '5', 'Swift/RTGS',
                  'Other')
             so_method_ind,
          h.actual_payment_amnt actual_payment_amount,
          DECODE (h.stand_order_method,
                  '0', curr0.short_descr,
                  '1', curr1.short_descr,
                  '5', swi.odr_cur_desc,
                  'Other')
             currency_code,
          t.first_payment_date,
          t.activation_date next_payment_date,
          t.last_payment_date expiration_date,
          dr.account_number debit_account_number,
          drd.designation debit_account_designation,
          beneficiaries_name debit_account_beneficiaries,
          drp.description debit_account_product,
          drtype.description debit_account_type,
          DECODE (h.stand_order_method, '1', cr.account_number, '')
             credit_account_number,
          DECODE (h.stand_order_method, '1', crd.designation, '')
             credit_account_designation,
          DECODE (h.stand_order_method, '1', crtype.description, '')
             credit_account_type,
          DECODE (h.stand_order_method, '1', crp.description, '')
             credit_account_product,
          TRIM (dcust.first_name) || ' ' || TRIM (dcust.surname)
             originator_name,
          TRIM (ccust.first_name) || ' ' || TRIM (ccust.surname)
             beneficiary_name
     FROM hist_so_commitment h
          LEFT JOIN currency curr1 ON h.cr_acc_currency = curr1.id_currency
          LEFT JOIN ips_message_stage ips
             ON h.tp_so_identifier = ips.tp_so_identifier
          LEFT JOIN currency curr0 ON ips.order_currency = curr0.id_currency
          LEFT JOIN hist_so_swift_103 swi
             ON     h.tp_so_identifier = swi.tp_so_identifier
                AND h.activation_date = swi.activation_date
          INNER JOIN tp_so_commitment t
             ON h.tp_so_identifier = t.tp_so_identifier
          INNER JOIN profits_account dr
             ON     dr.dep_acc_number = h.dr_account_number
                AND dr.prft_system = 3
                AND dr.SECONDARY_ACC <> '1'
          LEFT JOIN product drp ON dr.product_id = drp.id_product
          LEFT JOIN profits_account cr
             ON     cr.dep_acc_number = h.cr_account_number
                AND cr.prft_system = 3
                AND cr.SECONDARY_ACC <> '1'
          LEFT JOIN product crp ON cr.product_id = crp.id_product
          LEFT JOIN deposit_account drd
             ON drd.account_number = h.dr_account_number
          LEFT JOIN deposit dp
             ON dp.fk_productid_produ = drd.fk_depositfk_produ
          LEFT JOIN generic_detail drtype
             ON     DP.FK_GENERIC_DETASER = drtype.serial_num
                AND drtype.fk_generic_headpar = 'LACTP'
                AND drtype.fk_generic_headpar = dp.fk_generic_detafk
          LEFT JOIN deposit_account crd
             ON crd.account_number = h.cr_account_number
          LEFT JOIN deposit cp
             ON cp.fk_productid_produ = crd.fk_depositfk_produ
          LEFT JOIN generic_detail crtype
             ON     cp.FK_GENERIC_DETASER = crtype.serial_num
                AND crtype.fk_generic_headpar = 'LACTP'
                AND crtype.fk_generic_headpar = cp.fk_generic_detafk
          LEFT JOIN t ON t.fk_deposit_accoacc = dr.dep_acc_number
          LEFT JOIN customer dcust ON dcust.cust_id = h.dr_acc_customer_id
          LEFT JOIN customer ccust ON ccust.cust_id = h.cr_acc_customer_id
    WHERE h.tp_so_identifier >= 0;

