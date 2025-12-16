CREATE VIEW eom_loans  AS     SELECT *       FROM w_eom_loan_account      WHERE closed_flag = 'No';

