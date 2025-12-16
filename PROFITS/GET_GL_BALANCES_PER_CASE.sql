CREATE FUNCTION get_gl_balances_per_case(p_gl_account_from VARCHAR(21)
																	 ,p_gl_account_to VARCHAR(21)
																	 ,p_unit VARCHAR(10)
																	 ,p_currency VARCHAR(10)
																	 ,p_balance_type VARCHAR(40)
																	 ,p_date_from DATE
																	 ,p_date_from_type VARCHAR(40)
																	 ,p_date_to DATE
																	 ,p_date_to_type VARCHAR(40)
																	 ,p_fixing_date DATE
																	 ,p_converted VARCHAR(40)
																	 ,p_level VARCHAR(40)
																	 ,p_original_sign VARCHAR(40)
																	 ,p_calc_type VARCHAR(40))
		RETURNS DECIMAL(30, 2)
	BEGIN
		DECLARE v_balance DECIMAL(30, 2);
		DECLARE v_balance_from DECIMAL(30, 2);
		DECLARE v_balance_to DECIMAL(30, 2);
		DECLARE v_date_from DATE;
		DECLARE v_date_to DATE;
		DECLARE v_lc_indicator VARCHAR(2);
		DECLARE v_zb_indicator VARCHAR(2);
		DECLARE v_factor DECIMAL(1);
 
		SET v_date_from = gl_pkg.get_desired_gl_date(p_date_from, p_date_from_type);
		SET v_date_to = gl_pkg.get_desired_gl_date(p_date_to, p_date_to_type);
		SET v_lc_indicator = CASE p_converted WHEN 'YES' THEN 'LC' ELSE 'FC' END;
		SET v_zb_indicator =
			CASE p_balance_type
				WHEN 'BALANCE' THEN '0'
				WHEN 'BALANCE_ZB' THEN '1'
				WHEN 'DEBIT' THEN '2'
				WHEN 'DEBIT_BD' THEN '4'
				WHEN 'DEBIT_ZB' THEN '6'
				WHEN 'CREDIT' THEN '3'
				WHEN 'CREDIT_BD' THEN '5'
				WHEN 'CREDIT_ZB' THEN '7'
				WHEN 'DEBIT_PLUS_BD' THEN '8'
				WHEN 'CREDIT_PLUS_BD' THEN '9'
				WHEN 'DEBIT_ALL' THEN '10'
				WHEN 'CREDIT_ALL' THEN '11'
				ELSE '0'
			END;
		SET v_balance = 0;
		FOR accounts
			AS (SELECT	 account_id, b.fkgd_has_a_primary AS mon_type
					FROM		 glg_account a, par_relation_detai b, generic_detail c
					WHERE 			 a.subs_cons_flag = 1
									 AND TRIM(a.account_id) BETWEEN p_gl_account_from
																							AND p_gl_account_to
									 AND c.parameter_type = 'GLKIN'
									 AND a.account_id LIKE TRIM(c.description) || '%'
									 AND b.fk_par_relationcod = 'GLMONKIN'
									 AND b.fkgd_has_a_seconda = c.serial_num
					ORDER BY 1)
		DO
			IF p_calc_type = 'DIF'
			THEN
				SET v_balance_to =
					gl_pkg.get_gl_balance(accounts.account_id
															 ,p_currency
															 ,p_unit
															 ,v_date_to
															 ,p_fixing_date
															 ,v_lc_indicator
															 ,v_zb_indicator);
				SET v_balance_from =
					gl_pkg.get_gl_balance(accounts.account_id
															 ,p_currency
															 ,p_unit
															 ,v_date_from
															 ,p_fixing_date
															 ,v_lc_indicator
															 ,v_zb_indicator);
			END IF;
			IF p_calc_type = 'END'
			THEN
				SET v_balance_to =
					gl_pkg.get_gl_balance(accounts.account_id
															 ,p_currency
															 ,p_unit
															 ,v_date_to
															 ,p_fixing_date
															 ,v_lc_indicator
															 ,v_zb_indicator);
			END IF;
			IF p_calc_type = 'ADD'
			THEN
				SELECT SUM(gl_pkg.get_gl_balance(accounts.account_id
																				,p_currency
																				,p_unit
																				,a.date_id
																				,p_fixing_date
																				,v_lc_indicator
																				,v_zb_indicator))
				INTO	 v_balance_to
				FROM	 calendar a
				WHERE  a.date_id BETWEEN p_date_from AND p_date_to;
			END IF;
			SET v_factor =
				CASE
					WHEN		 p_balance_type IN ('BALANCE', 'BALANCE_ZB')
							 AND p_original_sign = 'YES'
							 AND accounts.mon_type = 2
					THEN
						-1
					ELSE
						1
				END;
			SET v_balance =
					v_balance
				+ (v_factor * (NVL(v_balance_to, 0) - NVL(v_balance_from, 0)));
		END FOR;
		RETURN NVL(v_balance, 0);
	END;

