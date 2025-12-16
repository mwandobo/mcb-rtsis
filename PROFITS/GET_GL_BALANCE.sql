CREATE FUNCTION GET_GL_BALANCE (
    INGLACCOUNT	VARCHAR(20),
    INCURRENCYID	VARCHAR(20),
    INUNITID	VARCHAR(20),
    INBALANCE_DATE	DATE,
    IN_RATE_DATE	DATE,
    INLC_INDICATOR	VARCHAR(20),
    INZB_INDICATOR	VARCHAR(20) )
  RETURNS DECIMAL(30, 2)
  SPECIFIC SQL160729100026608
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN
  declare  v_balance                DECIMAL(30,2) default 0;
  declare  v_balance_zb             DECIMAL(30,2);
  declare   v_debit                     DECIMAL(30,2);
  declare  v_debit_bd                DECIMAL(30,2);
  declare  v_debit_zb                  DECIMAL(30,2);
  declare   v_credit                 DECIMAL(30,2);
  declare  v_credit_bd                 DECIMAL(30,2);
  declare   v_credit_zb                 DECIMAL(30,2);
  declare     v_acc_kind                INTEGER;

 SELECT  NVL (SUM (w.balance* rate) , 0)
        ,NVL (SUM (w.balance_zb* rate) , 0)
        ,NVL (SUM (w.debit* rate) , 0)
        ,NVL (SUM (w.debit_bd* rate) , 0)
        ,NVL (SUM (w.debit_zb * rate), 0)
        ,NVL (SUM (w.credit* rate) , 0)
        ,NVL (SUM (w.credit_bd* rate) , 0)
        ,NVL (SUM (w.credit_zb* rate) , 0)
INTO     v_balance
        ,v_balance_zb
        ,v_debit
        ,v_debit_bd
        ,v_debit_zb
        ,v_credit
        ,v_credit_bd
        ,v_credit_zb
FROM     w_agg_gl_unit_totals w
         INNER JOIN w_eom_fixing_rate
            ON (    w.fk_currencyid_curr = w_eom_fixing_rate.currency_id
                AND w_eom_fixing_rate.eom_date = in_rate_date)
WHERE        w.fk_company_code = 1000
         AND TRIM (w.fk_glg_accountacco) = TRIM (inglaccount)
         AND w.fk_cost_id = '          '
         AND (   (w.gl_date = inbalance_date AND inzb_indicator NOT IN ('0', '1'))
              OR (    w.gl_date =
                         (SELECT MAX (w1.gl_date)
                          FROM   w_agg_gl_unit_totals w1
                          WHERE      w1.fk_company_code = w.fk_company_code
                                 AND w1.fk_glg_accountacco = w.fk_glg_accountacco
                                 AND w1.fk_currencyid_curr = w.fk_currencyid_curr
                                 AND w1.fk_unitcode = w.fk_unitcode
                                 AND w1.fk_cost_id = w.fk_cost_id
                                 AND w1.gl_date <= inbalance_date)
                  AND inzb_indicator IN ('0', '1')))
                AND    
                 CASE WHEN incurrencyid = 'ALL' AND inunitid = 'ALL' THEN 1 ELSE        
                   CASE WHEN incurrencyid = 'ALL' AND inunitid <> 'ALL' AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE 
                     CASE WHEN incurrencyid = 'FC' AND inunitid = 'ALL' AND w.fk_currencyid_curr <> (SELECT a.id_currency  FROM   currency a WHERE  a.national_flag = '1') THEN 1 ELSE 
                       CASE WHEN incurrencyid = 'FC' AND inunitid <> 'ALL' AND w.fk_currencyid_curr <> (SELECT a.id_currency FROM   currency a WHERE  a.national_flag = '1') AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE
                         CASE WHEN incurrencyid NOT IN ('ALL', 'FC') AND inunitid = 'ALL' AND inlc_indicator = 'FC' AND w.fk_currencyid_curr = TO_NUMBER (incurrencyid) THEN 1 ELSE
                           CASE WHEN incurrencyid NOT IN ('ALL', 'FC')  AND inunitid <> 'ALL'  AND inlc_indicator = 'FC' AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE
                             CASE WHEN incurrencyid NOT IN ('ALL', 'FC')      AND inunitid = 'ALL'      AND inlc_indicator = 'LC' THEN 1 ELSE 
                               CASE WHEN incurrencyid NOT IN ('ALL', 'FC') AND inunitid <> 'ALL'  AND inlc_indicator = 'LC' AND w.fk_unitcode = TO_NUMBER (inunitid)AND w.fk_currencyid_curr = TO_NUMBER (incurrencyid) THEN 1 ELSE 0 END
                            END
                          END
                        END
                      END
                    END
                  END
                END                
              = 1;

    if inzb_indicator = '1'
    then
     SET  v_balance = v_balance_zb;
    end if;
    if inzb_indicator = '2'
    then
     SET  v_balance = v_debit;
    end if;
    if inzb_indicator = '3'
    then
    SET   v_balance = v_credit;
    end if;
    if inzb_indicator = '4'
    then
     SET  v_balance = v_debit_bd;
    end if;
    if inzb_indicator = '5'
    then
    SET   v_balance = v_credit_bd;
    end if;
    if inzb_indicator = '6'
    then
     SET  v_balance = v_debit_zb;
    end if;
    if inzb_indicator = '7'
    then
    SET   v_balance = v_credit_zb;
    end if;
    if inzb_indicator = '8'
    then
     SET  v_balance = v_debit + v_debit_bd;
    end if;
    if inzb_indicator = '9'
    then
     SET  v_balance = v_credit + v_credit_bd;
    end if;
    if inzb_indicator = '10'
    then
     SET  v_balance = v_debit + v_debit_bd + v_debit_zb;
    end if;
    if inzb_indicator = '11'
    then
     SET  v_balance = v_credit + v_credit_bd + v_credit_zb;
    end if;

     SELECT NVL (a.fkgd_has_a_primary, 1)
     INTO   v_acc_kind
     FROM   par_relation_detai a
            RIGHT JOIN generic_detail c ON (a.fkgd_has_a_seconda = c.serial_num)
            INNER JOIN glg_account b
               ON (b.account_id LIKE TRIM (c.description) || '%')
     WHERE      c.fk_generic_headpar = 'GLKIN'
            AND TRIM (b.account_id) = TRIM (inglaccount)
            AND a.fk_par_relationcod = 'GLMONKIN';

      IF v_acc_kind IN ('2')
      THEN
       SET  v_balance = -1 * v_balance;
      END IF;
      
      RETURN v_balance;

      
END
ALTER MODULE PROFITS.GL_PKG PUBLISH
  FUNCTION get_gl_balance(IN p_glaccount  VARCHAR(21)
												 ,IN p_currencyid VARCHAR(21)
												 ,IN p_unitid VARCHAR(21)
												 ,IN p_balance_date DATE
												 ,IN p_rate_date DATE
												 ,IN p_local_currency_indicator VARCHAR(21)
												 ,IN p_zb_indicator VARCHAR(21))
		RETURNS DECIMAL(30, 2)
	BEGIN
		DECLARE v_balance DECIMAL(30, 2);
		DECLARE v_acc_kind DECIMAL(5);
    SET v_balance = 0;
		SELECT NVL(a.fkgd_has_a_primary, 1)
		INTO	 v_acc_kind
		FROM	 glg_account b, par_relation_detai a, generic_detail c
		WHERE 		 c.fk_generic_headpar = 'GLKIN'
					 AND TRIM(b.account_id) = TRIM(p_glaccount)
					 AND b.account_id LIKE TRIM(c.description) || '%'
					 AND a.fk_par_relationcod = 'GLMONKIN'
					 AND a.fkgd_has_a_seconda = c.serial_num;
		IF v_acc_kind = '2'
		THEN
			IF p_zb_indicator IN ('2'
													 ,'3'
													 ,'4'
													 ,'5'
													 ,'6'
													 ,'7'
													 ,'8'
													 ,'9'
													 ,'10'
													 ,'11')
			THEN
				SET v_acc_kind = '1';
			END IF;
		END IF;
		IF p_currencyid NOT IN ('ALL', 'FC') AND p_local_currency_indicator = 'FC'
		THEN
			SELECT SUM(
							 CASE p_zb_indicator
								 WHEN '1' THEN w.balance_zb
								 WHEN '2' THEN w.debit
								 WHEN '3' THEN w.credit
								 WHEN '4' THEN w.debit_bd
								 WHEN '5' THEN w.credit_bd
								 WHEN '6' THEN w.debit_zb
								 WHEN '7' THEN w.credit_zb
								 WHEN '8' THEN w.debit + w.debit_bd
								 WHEN '9' THEN w.credit + w.credit_bd
								 WHEN '10' THEN w.debit + w.debit_bd + w.debit_zb
								 WHEN '11' THEN w.credit + w.credit_bd + w.credit_zb
								 ELSE w.balance
							 END)
			INTO	 v_balance
			FROM	 w_agg_gl_unit_totals w
			WHERE 		 w.fk_company_code = 1000
						 AND TRIM(w.fk_glg_accountacco) = TRIM(p_glaccount)
						 AND w.fk_cost_id = '          '
						 AND (	 (		w.gl_date = p_balance_date
											AND p_zb_indicator NOT IN ('0', '1'))
									OR (		w.gl_date =
														(SELECT MAX(w1.gl_date)
														 FROM 	w_agg_gl_unit_totals w1
														 WHERE			w1.fk_company_code = w.fk_company_code
																		AND w1.fk_glg_accountacco =
																					w.fk_glg_accountacco
																		AND w1.fk_currencyid_curr =
																					w.fk_currencyid_curr
																		AND w1.fk_unitcode = w.fk_unitcode
																		AND w1.fk_cost_id = w.fk_cost_id
																		AND w1.gl_date <= p_balance_date)
											AND p_zb_indicator IN ('0', '1')))
						 AND w.fk_currencyid_curr = TO_NUMBER(p_currencyid)
						 AND w.fk_unitcode =
									 DECODE(p_unitid, 'ALL', w.fk_unitcode, TO_NUMBER(p_unitid));
		ELSE
			SELECT SUM(
								 CASE p_zb_indicator
									 WHEN '1' THEN w.balance_zb
									 WHEN '2' THEN w.debit
									 WHEN '3' THEN w.credit
									 WHEN '4' THEN w.debit_bd
									 WHEN '5' THEN w.credit_bd
									 WHEN '6' THEN w.debit_zb
									 WHEN '7' THEN w.credit_zb
									 WHEN '8' THEN w.debit + w.debit_bd
									 WHEN '9' THEN w.credit + w.credit_bd
									 WHEN '10' THEN w.debit + w.debit_bd + w.debit_zb
									 WHEN '11' THEN w.credit + w.credit_bd + w.credit_zb
									 ELSE w.balance
								 END
							 * wf.rate)
			INTO	 v_balance
			FROM	 w_agg_gl_unit_totals w
						 LEFT JOIN w_eom_fixing_rate wf
							 ON 		wf.eom_date = p_rate_date
									AND wf.currency_id = w.fk_currencyid_curr
						 LEFT JOIN currency localcurr
							 ON 		localcurr.id_currency = w.fk_currencyid_curr
									AND localcurr.national_flag = '1'
			WHERE 		 w.fk_company_code = 1000
						 AND TRIM(w.fk_glg_accountacco) = TRIM(p_glaccount)
						 AND w.fk_cost_id = '          '
						 AND (	 (		w.gl_date = p_balance_date
											AND p_zb_indicator NOT IN ('0', '1'))
									OR (		w.gl_date =
														(SELECT MAX(w1.gl_date)
														 FROM 	w_agg_gl_unit_totals w1
														 WHERE			w1.fk_company_code = w.fk_company_code
																		AND w1.fk_glg_accountacco =
																					w.fk_glg_accountacco
																		AND w1.fk_currencyid_curr =
																					w.fk_currencyid_curr
																		AND w1.fk_unitcode = w.fk_unitcode
																		AND w1.fk_cost_id = w.fk_cost_id
																		AND w1.gl_date <= p_balance_date)
											AND p_zb_indicator IN ('0', '1')))
						 AND CASE
									 WHEN (p_currencyid = 'ALL' AND p_unitid = 'ALL')
									 THEN
										 1
									 ELSE
										 CASE
											 WHEN 		(p_currencyid = 'ALL' AND p_unitid <> 'ALL')
														AND w.fk_unitcode = TO_NUMBER(p_unitid)
											 THEN
												 1
											 ELSE
												 CASE
													 WHEN 		(p_currencyid = 'FC' AND p_unitid = 'ALL')
																AND w.fk_currencyid_curr <>
																			localcurr.id_currency
													 THEN
														 1
													 ELSE
														 CASE
															 WHEN 		( 	 p_currencyid = 'FC'
																				 AND p_unitid <> 'ALL')
																		AND w.fk_currencyid_curr <>
																					localcurr.id_currency
																		AND w.fk_unitcode = TO_NUMBER(p_unitid)
															 THEN
																 1
															 ELSE
																 CASE
																	 WHEN 		( 	 p_currencyid NOT IN ('ALL'
																																		 ,'FC')
																						 AND p_unitid = 'ALL'
																						 AND p_local_currency_indicator =
																									 'LC')
																				AND w.fk_currencyid_curr =
																							TO_NUMBER(p_currencyid)
																	 THEN
																		 1
																	 ELSE
																		 CASE
																			 WHEN 		( 	 p_currencyid NOT IN ('ALL'
																																				 ,'FC')
																								 AND p_unitid <> 'ALL'
																								 AND p_local_currency_indicator =
																											 'LC')
																						AND w.fk_unitcode =
																									TO_NUMBER(p_unitid)
																						AND w.fk_currencyid_curr =
																									TO_NUMBER(p_currencyid)
																			 THEN
																				 1
																		 END
																 END
														 END
												 END
										 END
								 END = 1;
		END IF;
		IF v_acc_kind = '2'
		THEN
			SET v_balance = -1 * v_balance;
		END IF;
		RETURN NVL(v_balance, 0);
	END;

CREATE FUNCTION GET_GL_BALANCE (
    INGLACCOUNT	VARCHAR(20),
    INCURRENCYID	VARCHAR(20),
    INUNITID	VARCHAR(20),
    INBALANCE_DATE	DATE,
    IN_RATE_DATE	DATE,
    INLC_INDICATOR	VARCHAR(20),
    INZB_INDICATOR	VARCHAR(20) )
  RETURNS DECIMAL(30, 2)
  SPECIFIC SQL160729100026608
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN
  declare  v_balance                DECIMAL(30,2) default 0;
  declare  v_balance_zb             DECIMAL(30,2);
  declare   v_debit                     DECIMAL(30,2);
  declare  v_debit_bd                DECIMAL(30,2);
  declare  v_debit_zb                  DECIMAL(30,2);
  declare   v_credit                 DECIMAL(30,2);
  declare  v_credit_bd                 DECIMAL(30,2);
  declare   v_credit_zb                 DECIMAL(30,2);
  declare     v_acc_kind                INTEGER;

 SELECT  NVL (SUM (w.balance* rate) , 0)
        ,NVL (SUM (w.balance_zb* rate) , 0)
        ,NVL (SUM (w.debit* rate) , 0)
        ,NVL (SUM (w.debit_bd* rate) , 0)
        ,NVL (SUM (w.debit_zb * rate), 0)
        ,NVL (SUM (w.credit* rate) , 0)
        ,NVL (SUM (w.credit_bd* rate) , 0)
        ,NVL (SUM (w.credit_zb* rate) , 0)
INTO     v_balance
        ,v_balance_zb
        ,v_debit
        ,v_debit_bd
        ,v_debit_zb
        ,v_credit
        ,v_credit_bd
        ,v_credit_zb
FROM     w_agg_gl_unit_totals w
         INNER JOIN w_eom_fixing_rate
            ON (    w.fk_currencyid_curr = w_eom_fixing_rate.currency_id
                AND w_eom_fixing_rate.eom_date = in_rate_date)
WHERE        w.fk_company_code = 1000
         AND TRIM (w.fk_glg_accountacco) = TRIM (inglaccount)
         AND w.fk_cost_id = '          '
         AND (   (w.gl_date = inbalance_date AND inzb_indicator NOT IN ('0', '1'))
              OR (    w.gl_date =
                         (SELECT MAX (w1.gl_date)
                          FROM   w_agg_gl_unit_totals w1
                          WHERE      w1.fk_company_code = w.fk_company_code
                                 AND w1.fk_glg_accountacco = w.fk_glg_accountacco
                                 AND w1.fk_currencyid_curr = w.fk_currencyid_curr
                                 AND w1.fk_unitcode = w.fk_unitcode
                                 AND w1.fk_cost_id = w.fk_cost_id
                                 AND w1.gl_date <= inbalance_date)
                  AND inzb_indicator IN ('0', '1')))
                AND    
                 CASE WHEN incurrencyid = 'ALL' AND inunitid = 'ALL' THEN 1 ELSE        
                   CASE WHEN incurrencyid = 'ALL' AND inunitid <> 'ALL' AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE 
                     CASE WHEN incurrencyid = 'FC' AND inunitid = 'ALL' AND w.fk_currencyid_curr <> (SELECT a.id_currency  FROM   currency a WHERE  a.national_flag = '1') THEN 1 ELSE 
                       CASE WHEN incurrencyid = 'FC' AND inunitid <> 'ALL' AND w.fk_currencyid_curr <> (SELECT a.id_currency FROM   currency a WHERE  a.national_flag = '1') AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE
                         CASE WHEN incurrencyid NOT IN ('ALL', 'FC') AND inunitid = 'ALL' AND inlc_indicator = 'FC' AND w.fk_currencyid_curr = TO_NUMBER (incurrencyid) THEN 1 ELSE
                           CASE WHEN incurrencyid NOT IN ('ALL', 'FC')  AND inunitid <> 'ALL'  AND inlc_indicator = 'FC' AND w.fk_unitcode = TO_NUMBER (inunitid) THEN 1 ELSE
                             CASE WHEN incurrencyid NOT IN ('ALL', 'FC')      AND inunitid = 'ALL'      AND inlc_indicator = 'LC' THEN 1 ELSE 
                               CASE WHEN incurrencyid NOT IN ('ALL', 'FC') AND inunitid <> 'ALL'  AND inlc_indicator = 'LC' AND w.fk_unitcode = TO_NUMBER (inunitid)AND w.fk_currencyid_curr = TO_NUMBER (incurrencyid) THEN 1 ELSE 0 END
                            END
                          END
                        END
                      END
                    END
                  END
                END                
              = 1;

    if inzb_indicator = '1'
    then
     SET  v_balance = v_balance_zb;
    end if;
    if inzb_indicator = '2'
    then
     SET  v_balance = v_debit;
    end if;
    if inzb_indicator = '3'
    then
    SET   v_balance = v_credit;
    end if;
    if inzb_indicator = '4'
    then
     SET  v_balance = v_debit_bd;
    end if;
    if inzb_indicator = '5'
    then
    SET   v_balance = v_credit_bd;
    end if;
    if inzb_indicator = '6'
    then
     SET  v_balance = v_debit_zb;
    end if;
    if inzb_indicator = '7'
    then
    SET   v_balance = v_credit_zb;
    end if;
    if inzb_indicator = '8'
    then
     SET  v_balance = v_debit + v_debit_bd;
    end if;
    if inzb_indicator = '9'
    then
     SET  v_balance = v_credit + v_credit_bd;
    end if;
    if inzb_indicator = '10'
    then
     SET  v_balance = v_debit + v_debit_bd + v_debit_zb;
    end if;
    if inzb_indicator = '11'
    then
     SET  v_balance = v_credit + v_credit_bd + v_credit_zb;
    end if;

     SELECT NVL (a.fkgd_has_a_primary, 1)
     INTO   v_acc_kind
     FROM   par_relation_detai a
            RIGHT JOIN generic_detail c ON (a.fkgd_has_a_seconda = c.serial_num)
            INNER JOIN glg_account b
               ON (b.account_id LIKE TRIM (c.description) || '%')
     WHERE      c.fk_generic_headpar = 'GLKIN'
            AND TRIM (b.account_id) = TRIM (inglaccount)
            AND a.fk_par_relationcod = 'GLMONKIN';

      IF v_acc_kind IN ('2')
      THEN
       SET  v_balance = -1 * v_balance;
      END IF;
      
      RETURN v_balance;

      
END
ALTER MODULE PROFITS.GL_PKG PUBLISH
  FUNCTION get_gl_balance(IN p_glaccount  VARCHAR(21)
												 ,IN p_currencyid VARCHAR(21)
												 ,IN p_unitid VARCHAR(21)
												 ,IN p_balance_date DATE
												 ,IN p_rate_date DATE
												 ,IN p_local_currency_indicator VARCHAR(21)
												 ,IN p_zb_indicator VARCHAR(21))
		RETURNS DECIMAL(30, 2)
	BEGIN
		DECLARE v_balance DECIMAL(30, 2);
		DECLARE v_acc_kind DECIMAL(5);
    SET v_balance = 0;
		SELECT NVL(a.fkgd_has_a_primary, 1)
		INTO	 v_acc_kind
		FROM	 glg_account b, par_relation_detai a, generic_detail c
		WHERE 		 c.fk_generic_headpar = 'GLKIN'
					 AND TRIM(b.account_id) = TRIM(p_glaccount)
					 AND b.account_id LIKE TRIM(c.description) || '%'
					 AND a.fk_par_relationcod = 'GLMONKIN'
					 AND a.fkgd_has_a_seconda = c.serial_num;
		IF v_acc_kind = '2'
		THEN
			IF p_zb_indicator IN ('2'
													 ,'3'
													 ,'4'
													 ,'5'
													 ,'6'
													 ,'7'
													 ,'8'
													 ,'9'
													 ,'10'
													 ,'11')
			THEN
				SET v_acc_kind = '1';
			END IF;
		END IF;
		IF p_currencyid NOT IN ('ALL', 'FC') AND p_local_currency_indicator = 'FC'
		THEN
			SELECT SUM(
							 CASE p_zb_indicator
								 WHEN '1' THEN w.balance_zb
								 WHEN '2' THEN w.debit
								 WHEN '3' THEN w.credit
								 WHEN '4' THEN w.debit_bd
								 WHEN '5' THEN w.credit_bd
								 WHEN '6' THEN w.debit_zb
								 WHEN '7' THEN w.credit_zb
								 WHEN '8' THEN w.debit + w.debit_bd
								 WHEN '9' THEN w.credit + w.credit_bd
								 WHEN '10' THEN w.debit + w.debit_bd + w.debit_zb
								 WHEN '11' THEN w.credit + w.credit_bd + w.credit_zb
								 ELSE w.balance
							 END)
			INTO	 v_balance
			FROM	 w_agg_gl_unit_totals w
			WHERE 		 w.fk_company_code = 1000
						 AND TRIM(w.fk_glg_accountacco) = TRIM(p_glaccount)
						 AND w.fk_cost_id = '          '
						 AND (	 (		w.gl_date = p_balance_date
											AND p_zb_indicator NOT IN ('0', '1'))
									OR (		w.gl_date =
														(SELECT MAX(w1.gl_date)
														 FROM 	w_agg_gl_unit_totals w1
														 WHERE			w1.fk_company_code = w.fk_company_code
																		AND w1.fk_glg_accountacco =
																					w.fk_glg_accountacco
																		AND w1.fk_currencyid_curr =
																					w.fk_currencyid_curr
																		AND w1.fk_unitcode = w.fk_unitcode
																		AND w1.fk_cost_id = w.fk_cost_id
																		AND w1.gl_date <= p_balance_date)
											AND p_zb_indicator IN ('0', '1')))
						 AND w.fk_currencyid_curr = TO_NUMBER(p_currencyid)
						 AND w.fk_unitcode =
									 DECODE(p_unitid, 'ALL', w.fk_unitcode, TO_NUMBER(p_unitid));
		ELSE
			SELECT SUM(
								 CASE p_zb_indicator
									 WHEN '1' THEN w.balance_zb
									 WHEN '2' THEN w.debit
									 WHEN '3' THEN w.credit
									 WHEN '4' THEN w.debit_bd
									 WHEN '5' THEN w.credit_bd
									 WHEN '6' THEN w.debit_zb
									 WHEN '7' THEN w.credit_zb
									 WHEN '8' THEN w.debit + w.debit_bd
									 WHEN '9' THEN w.credit + w.credit_bd
									 WHEN '10' THEN w.debit + w.debit_bd + w.debit_zb
									 WHEN '11' THEN w.credit + w.credit_bd + w.credit_zb
									 ELSE w.balance
								 END
							 * wf.rate)
			INTO	 v_balance
			FROM	 w_agg_gl_unit_totals w
						 LEFT JOIN w_eom_fixing_rate wf
							 ON 		wf.eom_date = p_rate_date
									AND wf.currency_id = w.fk_currencyid_curr
						 LEFT JOIN currency localcurr
							 ON 		localcurr.id_currency = w.fk_currencyid_curr
									AND localcurr.national_flag = '1'
			WHERE 		 w.fk_company_code = 1000
						 AND TRIM(w.fk_glg_accountacco) = TRIM(p_glaccount)
						 AND w.fk_cost_id = '          '
						 AND (	 (		w.gl_date = p_balance_date
											AND p_zb_indicator NOT IN ('0', '1'))
									OR (		w.gl_date =
														(SELECT MAX(w1.gl_date)
														 FROM 	w_agg_gl_unit_totals w1
														 WHERE			w1.fk_company_code = w.fk_company_code
																		AND w1.fk_glg_accountacco =
																					w.fk_glg_accountacco
																		AND w1.fk_currencyid_curr =
																					w.fk_currencyid_curr
																		AND w1.fk_unitcode = w.fk_unitcode
																		AND w1.fk_cost_id = w.fk_cost_id
																		AND w1.gl_date <= p_balance_date)
											AND p_zb_indicator IN ('0', '1')))
						 AND CASE
									 WHEN (p_currencyid = 'ALL' AND p_unitid = 'ALL')
									 THEN
										 1
									 ELSE
										 CASE
											 WHEN 		(p_currencyid = 'ALL' AND p_unitid <> 'ALL')
														AND w.fk_unitcode = TO_NUMBER(p_unitid)
											 THEN
												 1
											 ELSE
												 CASE
													 WHEN 		(p_currencyid = 'FC' AND p_unitid = 'ALL')
																AND w.fk_currencyid_curr <>
																			localcurr.id_currency
													 THEN
														 1
													 ELSE
														 CASE
															 WHEN 		( 	 p_currencyid = 'FC'
																				 AND p_unitid <> 'ALL')
																		AND w.fk_currencyid_curr <>
																					localcurr.id_currency
																		AND w.fk_unitcode = TO_NUMBER(p_unitid)
															 THEN
																 1
															 ELSE
																 CASE
																	 WHEN 		( 	 p_currencyid NOT IN ('ALL'
																																		 ,'FC')
																						 AND p_unitid = 'ALL'
																						 AND p_local_currency_indicator =
																									 'LC')
																				AND w.fk_currencyid_curr =
																							TO_NUMBER(p_currencyid)
																	 THEN
																		 1
																	 ELSE
																		 CASE
																			 WHEN 		( 	 p_currencyid NOT IN ('ALL'
																																				 ,'FC')
																								 AND p_unitid <> 'ALL'
																								 AND p_local_currency_indicator =
																											 'LC')
																						AND w.fk_unitcode =
																									TO_NUMBER(p_unitid)
																						AND w.fk_currencyid_curr =
																									TO_NUMBER(p_currencyid)
																			 THEN
																				 1
																		 END
																 END
														 END
												 END
										 END
								 END = 1;
		END IF;
		IF v_acc_kind = '2'
		THEN
			SET v_balance = -1 * v_balance;
		END IF;
		RETURN NVL(v_balance, 0);
	END;

