CREATE VIEW w_fact_gl_entep_total
AS
	WITH month_seq
			 AS (SELECT rnum month_seq_no
								 ,DECODE(rnum, 1, 1, 0) flag_01
								 ,DECODE(rnum, 2, 1, 0) flag_02
								 ,DECODE(rnum, 3, 1, 0) flag_03
								 ,DECODE(rnum, 4, 1, 0) flag_04
								 ,DECODE(rnum, 5, 1, 0) flag_05
								 ,DECODE(rnum, 6, 1, 0) flag_06
								 ,DECODE(rnum, 7, 1, 0) flag_07
								 ,DECODE(rnum, 8, 1, 0) flag_08
								 ,DECODE(rnum, 9, 1, 0) flag_09
								 ,DECODE(rnum, 10, 1, 0) flag_10
								 ,DECODE(rnum, 11, 1, 0) flag_11
								 ,DECODE(rnum, 12, 1, 0) flag_12
								 ,DECODE(rnum, 13, 1, 0) flag_13
					 FROM 	(SELECT ROW_NUMBER() OVER (ORDER BY 1) rnum FROM calendar)
					 WHERE	rnum <= 13)
	SELECT 'F' || LPAD(year0, 4, '0') || '-' || LPAD(month_seq_no, 2, '0')
					 fiscal_year_month
				,glg_unit_total.fk_currencyid_curr currency_id
				,glg_unit_total.fk_glg_accountacco gl_account
				,glg_unit_total.fk_unitcode unit_code
				,glg_unit_total.year0 fiscal_year
				,month_seq_no fiscal_month
				,level0
				,short_descr currency_code
				,  flag_01 * debit01
				 + flag_02 * (debit01 + debit02)
				 + flag_03 * (debit01 + debit02 + debit03)
				 + flag_04 * (debit01 + debit02 + debit03 + debit04)
				 + flag_05 * (debit01 + debit02 + debit03 + debit04 + debit05)
				 +	 flag_06
					 * (debit01 + debit02 + debit03 + debit04 + debit05 + debit06)
				 +	 flag_07
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07)
				 +	 flag_08
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08)
				 +	 flag_09
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08
							+ debit09)
				 +	 flag_10
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08
							+ debit09
							+ debit10)
				 +	 flag_11
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08
							+ debit09
							+ debit10
							+ debit11)
				 +	 flag_12
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08
							+ debit09
							+ debit10
							+ debit11
							+ debit12)
				 +	 flag_13
					 * (	debit01
							+ debit02
							+ debit03
							+ debit04
							+ debit05
							+ debit06
							+ debit07
							+ debit08
							+ debit09
							+ debit10
							+ debit11
							+ debit12
							+ bal_sheet_db)
					 debit_amount
				,  flag_01 * credit01
				 + flag_02 * (credit01 + credit02)
				 + flag_03 * (credit01 + credit02 + credit03)
				 + flag_04 * (credit01 + credit02 + credit03 + credit04)
				 + flag_05 * (credit01 + credit02 + credit03 + credit04 + credit05)
				 +	 flag_06
					 * (credit01 + credit02 + credit03 + credit04 + credit05 + credit06)
				 +	 flag_07
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07)
				 +	 flag_08
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08)
				 +	 flag_09
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08
							+ credit09)
				 +	 flag_10
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08
							+ credit09
							+ credit10)
				 +	 flag_11
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08
							+ credit09
							+ credit10
							+ credit11)
				 +	 flag_12
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08
							+ credit09
							+ credit10
							+ credit11
							+ credit12)
				 +	 flag_13
					 * (	credit01
							+ credit02
							+ credit03
							+ credit04
							+ credit05
							+ credit06
							+ credit07
							+ credit08
							+ credit09
							+ credit10
							+ credit11
							+ credit12
							+ bal_sheet_cr)
					 credit_amount
	FROM	 glg_unit_total
				 JOIN currency ON glg_unit_total.fk_currencyid_curr = id_currency
				 JOIN month_seq ON (1 = 1);

