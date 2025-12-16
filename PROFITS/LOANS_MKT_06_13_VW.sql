CREATE VIEW LOANS_MKT_06_13_VW  (     ACC_SN,     ACC_STATUS,     ACC_TYPE,     ACCOUNT_CD,     ACCOUNT_NUMBER,     BANK_SPREAD,     C_DIGIT,     CR_CNTR_GL_ACC,     CR_INT_ACCR_GL_ACC,     CR_INT_GL_ACC,     CURR_TRX_DATE,     CUST_ID,     B$DESCRIPTION,     GD$DESCRIPTION,     I$DESCRIPTION,     CG1$DR_CNTR_GL_ACC,     CG2$DR_CNTR_GL_ACC,     CG3$DR_CNTR_GL_ACC,     CLASS_GL$DR_CNTR_GL_ACC,     DR_INT_ACCR_GL_ACC,     DR_INT_GL_ACC,     FIRST_NAME,     FK_BASE_RATEFK_GD,     FK_INTERESTID_INTE,     FK_UNITCODE,     GL_ACCRUAL_ACC,     ID_PRODUCT,     LNS_OPEN_UNIT,     LOAN_STATUS,     NRM_ACCRUAL_AMN,     NRM_CAP_BAL,     NRM_COM_BAL,     NRM_EXP_BAL,     NRM_RL_INT_BAL,     NRM_RL_URL_FLG,     NRM_URL_INT_BAL,     OV_ACCRUAL_AMN,     OV_CAP_BAL,     OV_COM_BAL,     OV_EXP_BAL,     OV_EXP_DT,     OV_RL_NRM_INT_BAL,     OV_RL_PNL_INT_BAL,     OV_RL_URL_FLG,     OV_URL_NRM_INT_BAL,     OV_URL_PNL_INT_BAL,     N128$PERCENTAGE,     SC$PERCENTAGE,     POSITIVE_AMN,     SURNAME,     UNCLEAR_AMN,     UNIT_NAME,     CALCULAT_CURRENCY_FIX_RATE$,     MOVEMENT_CURRENCY  )  AS     SELECT la.acc_sn,            la.acc_status,            la.acc_type,            pa.account_cd,            pa.account_number,            sc.bank_spread,            c.c_digit,            class_gl.cr_cntr_gl_acc,            class_gl.cr_int_accr_gl_acc,            class_gl.cr_int_gl_acc,            p.curr_trx_date,            c.cust_id,            b.description b$description,            gd.description gd$description,            i.description i$description,            cg1.dr_cntr_gl_acc cg1$dr_cntr_gl_acc,            cg2.dr_cntr_gl_acc cg2$dr_cntr_gl_acc,            cg3.dr_cntr_gl_acc cg3$dr_cntr_gl_acc,            class_gl.dr_cntr_gl_acc class_gl$dr_cntr_gl_acc,            class_gl.dr_int_accr_gl_acc,            class_gl.dr_int_gl_acc,            c.first_name,            sc.fk_base_ratefk_gd,            sc.fk_interestid_inte,            la.fk_unitcode,            class_gl.gl_accrual_acc,            b.id_product,            pa.lns_open_unit,            la.loan_status,            a.nrm_accrual_amn,            la.nrm_cap_bal,            la.nrm_com_bal,            la.nrm_exp_bal,            la.nrm_rl_int_bal,            a.nrm_rl_url_flg,            la.nrm_url_int_bal,            a.ov_accrual_amn,            la.ov_cap_bal,            la.ov_com_bal,            la.ov_exp_bal,            la.ov_exp_dt,            la.ov_rl_nrm_int_bal,            la.ov_rl_pnl_int_bal,            a.ov_rl_url_flg,            la.ov_url_nrm_int_bal,            la.ov_url_pnl_int_bal,            n128.percentage n128$percentage,            sc.percentage sc$percentage,            a.positive_amn,            c.surname,            a.unclear_amn,            c.unit_name,            get_fixing_rate (pa.movement_currency, cast(NULL as date)) calculat_currency_fix_rate$,            pa.movement_currency       /*tab_to_string(t_varchar2_tab(
                             pa.lns_open_unit
                            ,c.unit_name
                            ,pa.account_number
                            ,pa.account_cd
                            ,c.cust_id
                            ,c.c_digit
                            ,c.surname
                            ,c.first_name
                            ,b.id_product
                            ,b.description
                            ,(la.nrm_cap_bal +
                              la.nrm_rl_int_bal +
                              la.nrm_exp_bal +
                              la.nrm_com_bal +
                              la.ov_cap_bal +
                              la.ov_rl_nrm_int_bal +
                              la.ov_rl_pnl_int_bal +
                              la.ov_exp_bal +
                              la.ov_com_bal)
                            ,(la.nrm_cap_bal + la.nrm_rl_int_bal + la.nrm_exp_bal + la.nrm_com_bal)
                            ,(la.ov_cap_bal + la.ov_rl_nrm_int_bal + la.ov_rl_pnl_int_bal + la.ov_exp_bal + la.ov_com_bal)
                            ,a.nrm_rl_url_flg
                            ,a.ov_rl_url_flg
                            ,RPAD(get_pan_due_cat(p.curr_trx_date, la.ov_exp_dt), 8, ' ')
                            ,(sc.percentage + n128.percentage + getloanspread(la.fk_unitcode, la.acc_type, la.acc_sn))
                            ,sc.fk_interestid_inte
                            ,i.description
                            ,sc.bank_spread
                            ,sc.fk_base_ratefk_gd
                            ,gd.description
                            ,(sc.percentage - sc.bank_spread)
                            ,getloanspread(la.fk_unitcode, la.acc_type, la.acc_sn)
                            ,n128.percentage
                            ,la.nrm_url_int_bal
                            ,(la.ov_url_nrm_int_bal + la.ov_url_pnl_int_bal)
                            ,a.positive_amn
                            ,a.unclear_amn
                            ,a.nrm_accrual_amn
                            ,a.ov_accrual_amn
                            ,class_gl.gl_accrual_acc
                            ,class_gl.cr_int_accr_gl_acc
                            ,class_gl.dr_int_accr_gl_acc
                            ,class_gl.cr_int_gl_acc
                            ,class_gl.dr_int_gl_acc
                            ,class_gl.dr_cntr_gl_acc
                            ,class_gl.cr_cntr_gl_acc
                            ,cg1.dr_cntr_gl_acc
                            ,cg2.dr_cntr_gl_acc
                            ,cg3.dr_cntr_gl_acc
                            ,la.loan_status
                            ,la.acc_status
                            ,(la.nrm_cap_bal +
                              la.nrm_rl_int_bal +
                              la.nrm_exp_bal +
                              la.nrm_com_bal +
                              la.ov_cap_bal +
                              la.ov_rl_nrm_int_bal +
                              la.ov_rl_pnl_int_bal +
                              la.ov_exp_bal +
                              la.ov_com_bal) *
                             get_fixing_rate(movement_currency)
                            ,(la.nrm_cap_bal + la.nrm_rl_int_bal + la.nrm_exp_bal + la.nrm_com_bal) *
                             get_fixing_rate(movement_currency)
                            ,(la.ov_cap_bal + la.ov_rl_nrm_int_bal + la.ov_rl_pnl_int_bal + la.ov_exp_bal + la.ov_com_bal) *
                             get_fixing_rate(movement_currency)
                            ,la.nrm_url_int_bal * get_fixing_rate(movement_currency)
                            ,(la.ov_url_nrm_int_bal + la.ov_url_pnl_int_bal) * get_fixing_rate(movement_currency)
                            ,a.positive_amn * get_fixing_rate(movement_currency)
                            ,a.unclear_amn * get_fixing_rate(movement_currency)
                            ,a.nrm_accrual_amn * get_fixing_rate(movement_currency)
                            ,a.ov_accrual_amn * get_fixing_rate(movement_currency)
                           --))
               AS concatenated*/       FROM class_gl class_gl,            class_gl cg1,            class_gl cg2,            class_gl cg3,            customer c,            loan_account la,            profits_account pa,            loan_account_info a,            product b,            unit c,            int_scale sc,            lns_interest i,            generic_detail gd,            bank_parameters p,            loan LN,            int_scale n128      WHERE     pa.lns_open_unit = la.fk_unitcode            AND pa.lns_sn = la.acc_sn            AND pa.lns_type = la.acc_type            AND la.fk_loanfk_producti = class_gl.fk_productid_produ            AND la.fkgd_category = class_gl.fk_cust_categ_gd            AND la.fkgd_has_as_class = class_gl.fk_generic_detaser            AND pa.cust_id = c.cust_id            AND la.fk_loanfk_producti = cg1.fk_productid_produ            AND la.fkgd_has_as_class = cg1.fk_generic_detaser            AND la.fkgd_has_as_class = cg2.fk_generic_detaser            AND la.fkgd_has_as_class = cg3.fk_generic_detaser            AND la.fk_loanfk_producti = cg2.fk_productid_produ            AND la.fk_loanfk_producti = cg3.fk_productid_produ            AND la.fkgd_category = cg1.fk_cust_categ_gd            AND la.fkgd_category = cg2.fk_cust_categ_gd            AND la.fkgd_category = cg3.fk_cust_categ_gd            AND (    (pa.prft_system = 4)                 AND (la.acc_status <> '3')                 AND (class_gl.loan_status = '1')                 AND (cg1.loan_status = '2')                 AND (cg2.loan_status = '3')                 AND (cg3.loan_status = '4'))            AND pa.lns_type <> 14            AND la.fk_unitcode = a.fk_loan_accountfk            AND la.acc_type = a.fk0loan_accountacc            AND la.acc_sn = a.fk_loan_accountacc            AND pa.product_id = b.id_product            AND c.code = pa.lns_open_unit            AND sc.fk_interestid_inte = i.id_interest            AND gd.parameter_type = 'BRATE'            AND gd.serial_num = sc.fk_base_ratefk_gd            AND i.id_interest = getratecode (p.curr_trx_date,                                             la.fkint_prev_fixed,                                             la.prv_fx_int_exp_dt,                                             la.fkint_current_fix,                                             la.cur_fx_int_exp_dt,                                             la.fkint_as_floating)            AND sc.validity_date = getratevaldate (p.curr_trx_date,                                                   la.prv_fx_int_scal_dt,                                                   la.prv_fx_int_exp_dt,                                                   la.cur_fx_int_scal_dt,                                                   la.cur_fx_int_exp_dt,                                                   la.fkint_as_floating,                                                   la.fkcur_is_moved_in)            AND sc.fk_currencyid_curr = la.fkcur_is_moved_in            AND la.fk_loanfk_producti = LN.fk_productid_produ            AND LN.fkint_has_n128_int = n128.fk_interestid_inte            AND n128.validity_date =                   (SELECT MAX (validity_date)                      FROM int_scale                     WHERE fk_interestid_inte = n128.fk_interestid_inte)            AND n128.fk_currencyid_curr = la.fkcur_is_moved_in;

