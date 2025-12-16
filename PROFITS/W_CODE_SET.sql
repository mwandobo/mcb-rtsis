create table W_CODE_SET
(
    CODE_SET_ID   DECIMAL(10),
    CODE_SET_NAME VARCHAR(50),
    CODE_SET_DESC VARCHAR(100)
);

create unique index PK_W_CODE_SET
    on W_CODE_SET (CODE_SET_ID);

CREATE PROCEDURE W_CODE_SET ( )
  SPECIFIC SQL160620112632440
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_code;
DELETE w_code_set;
INSERT INTO w_code_set (code_set_id, code_set_name, code_set_desc)
   SELECT 1 code_set_id
         ,'Identity Type' code_set_name
         ,description || ' -  Generic {OIDTP}' code_set_desc
   FROM   generic_header
   WHERE  parameter_type = 'OIDTP';
INSERT INTO w_code (code_set_id, code_value, code_desc)
   SELECT 1 code_set_id, serial_num code_value, description code_desc
   FROM   generic_detail
   WHERE  fk_generic_headpar = 'OIDTP';
INSERT INTO w_code_set (code_set_id, code_set_name, code_set_desc)
VALUES      (2, 'Deposits Status', 'Deposits Status');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '0', 'Deleted');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '1', 'Active');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '2', 'Locked');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '3', 'Closed by Cust.');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '4', 'Closed by Bank');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '5', 'Blocked');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '6', 'Dormant');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '7', 'Unfunded');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (2, '8', 'Inactive');
INSERT INTO w_code_set (code_set_id, code_set_name, code_set_desc)
   VALUES      (
                  3
                 ,'Deposits Account Type'
                 ,'Deposits Account Type (r_deposit_account.deposit_type)');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '1', 'First Demand');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '2', 'Time Deposit');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '3', 'Certificate');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '4', 'Instant Income');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '5', 'Overdraft');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '6', 'Commitment');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (3, '7', 'Regular Income');
INSERT INTO w_code_set (code_set_id, code_set_name, code_set_desc)
   VALUES      (
                  4
                 ,'Insurance Entry Status'
                 ,'Insurance Entry Status(Iss_Commitment.Entry_status)');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '06', 'Confirmed');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '07', 'Deleted');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '05', 'Canceled');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '04', 'Active from Loan Acc. Debit');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '03', 'Active from Successful Depos. Acct. Debit');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '02', 'Active from Unpaid');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '01', 'For Finalization');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (4, '08', 'Approved');
INSERT INTO w_code_set (code_set_id, code_set_name, code_set_desc)
VALUES      (5, 'Product Type', ' ');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '0', 'Trade Finance');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '1', 'Deposit Account');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '2', 'Loan Account');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '3', 'Service');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '4', 'Collateral');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '5', 'Agreement');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '6', 'Safe Deposit Box');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '7', 'Letter of Guarantee');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '8', 'Treasury Bond');
INSERT INTO w_code (code_set_id, code_value, code_desc)
VALUES      (5, '9', 'Insurance');
END;

