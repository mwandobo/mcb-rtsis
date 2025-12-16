CREATE VIEW eom_inactive_loans  AS     SELECT *       FROM w_eom_loan_account      WHERE closed_flag = 'Yes';

