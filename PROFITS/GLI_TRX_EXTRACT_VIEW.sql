CREATE VIEW GLI_TRX_EXTRACT_VIEW
(
   FK_GLG_ACCOUNTACCO,
   FK1UNITCODE,
   CURRENCY_SHORT_DES,
   TRX_USR,
   TRX_GL_TRN_DATE,
   FC_AMOUNT,
   ENTRY_COMMENTS,
   ENTRY_TYPE,
   SUBSYSTEM,
   ID_PRODUCT,
   TRX_CODE,
   ID_JUSTIFIC,
   JUSTIFIC_DESCR,
   PRF_ACCOUNT_NUMBER,
   PRF_ACC_CD,
   EXTERNAL_GLACCOUNT,
   CUST_ID,
   GL_RULE,
   GLACC_ORIGIN,
   AMOUNT_SER_NO,
   TRN_DATE,
   FK_UNITCODETRXUNIT,
   FK_USRCODE,
   TRX_SN,
   TUN_INTERNAL_SN,
   REMARKS,
   ACCOUNT_NUMBER,
   FK_GLG_JUSTIFYJUST,
   line_num,
   trn_snum
)
AS
   SELECT FK_GLG_ACCOUNTACCO,
          FK1UNITCODE,
          CURRENCY_SHORT_DES,
          TRX_USR,
          TRX_GL_TRN_DATE,
          FC_AMOUNT,
          ENTRY_COMMENTS,
          ENTRY_TYPE,
          SUBSYSTEM,
          ID_PRODUCT,
          TRX_CODE,
          ID_JUSTIFIC,
          justific_descr,
          PRF_ACCOUNT_NUMBER,
          PRF_ACC_CD,
          external_glaccount,
          cust_id,
          gl_rule,
          glacc_origin,
          amount_ser_no,
          trn_date,
          fk_unitcodetrxunit,
          fk_usrcode,
          trx_sn,
          tun_internal_sn,
          remarks,
          account_number,
		  FK_GLG_JUSTIFYJUST,
		  line_num,
		  trn_snum
     FROM (SELECT A.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
                  a.FK1UNITCODE AS FK1UNITCODE,
                  b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                  SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                     AS TRX_USR,
                  a.GL_TRN_DATE AS TRX_GL_TRN_DATE,
                  a.AMOUNT AS FC_AMOUNT,
                  A.FK_TERMINALTERMINA AS ENTRY_COMMENTS,
                  A.ENTRY_TYPE,
                  SUBSYSTEM,
                  0 AS ID_PRODUCT,
                  A.FK_TRANSACTION0ID AS TRX_CODE,
                  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
                  C.DESCRIPTION AS justific_descr,
                  TO_CHAR (prf_account_number) AS prf_account_number,
                  TO_NUMBER (prf_acc_cd) AS prf_acc_cd,
                  REPLACE (A.FK_GLG_ACCOUNTACCO, '.', '')
                     AS external_glaccount,
                  FK_CUSTOMERCODE AS cust_id,
                  0 AS gl_rule,
                  glacc_origin,
                  amount_ser_no,
                  trn_date,
                  fk_unitcode AS fk_unitcodetrxunit,
                  fk_usrcode,
                  TO_NUMBER (trx_usr_sn) AS trx_sn,
                  TO_NUMBER (tun_internal_sn) AS tun_internal_sn,
                  remarks,
                  TO_CHAR (account_number) AS account_number,
				  FK_GLG_JUSTIFYJUST,
				  a.line_num as line_num,
				  a.trn_snum as trn_snum
             FROM dep_gli_interface a
             --, currency b, JUSTIFIC C
          -- WHERE     b.id_currency = a.fk_currencyid_curr
           --       AND a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC(+)
             LEFT JOIN CURRENCY B
                    ON (A.FK_CURRENCYID_CURR = B.ID_CURRENCY)
                  LEFT JOIN JUSTIFIC C
                    ON (A.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC)
           UNION ALL
           SELECT A.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
                  a.FK1UNITCODE AS FK1UNITCODE,
                  b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                  SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                     AS TRX_USR,
                  a.GL_TRN_DATE AS TRX_GL_TRN_DATE,
                  a.AMOUNT AS FC_AMOUNT,
                  A.FK_TERMINALTERMINA AS ENTRY_COMMENTS,
                  A.ENTRY_TYPE,
                  SUBSYSTEM,
                  0 AS ID_PRODUCT,
                  A.FK_TRANSACTION0ID AS TRX_CODE,
                  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
                  C.DESCRIPTION AS justific_descr,
                  TO_CHAR (PRF_ACCOUNT_NUMBER) AS prf_account_number,
                  TO_NUMBER (PRF_ACC_CD) AS prf_acc_cd,
                  REPLACE (A.FK_GLG_ACCOUNTACCO, '.', '')
                     AS external_glaccount,
                  FK_CUSTOMERCODE AS cust_id,
                  0 AS gl_rule,
                  glacc_origin,
                  amount_ser_no,
                  trn_date,
                  fk_unitcode AS fk_unitcodetrxunit,
                  fk_usrcode,
                  TO_NUMBER (
                     SUBSTR (remarks, INSTR (remarks, '-', 1) - 8, 8))
                     AS trx_sn,
                  TO_NUMBER (
                     SUBSTR (
                        remarks,
                        INSTR (remarks, '-', 1) + 1,
                        LENGTH (TRIM (remarks)) - (INSTR (remarks, '-', 1))))
                     AS tun_internal_sn,
                  remarks,
                  TO_CHAR (account_number) AS account_number,
				  FK_GLG_JUSTIFYJUST,
				  a.line_num as line_num,
				  a.trn_snum as trn_snum
             FROM lns_gli_interface a
             --, currency b, JUSTIFIC C
           -- WHERE     b.id_currency = a.fk_currencyid_curr
           --      AND a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC(+)
             LEFT JOIN CURRENCY B
                    ON (A.FK_CURRENCYID_CURR = B.ID_CURRENCY)
                  LEFT JOIN JUSTIFIC C
                    ON (A.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC)
           UNION ALL
           SELECT A.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
                  a.FK1UNITCODE AS FK1UNITCODE,
                  b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                  SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                     AS TRX_USR,
                  a.GL_TRN_DATE AS TRX_GL_TRN_DATE,
                  a.AMOUNT AS FC_AMOUNT,
                  A.FK_TERMINALTERMINA AS ENTRY_COMMENTS,
                  A.ENTRY_TYPE,
                  A.SUBSYSTEM,
                  0 AS ID_PRODUCT,
                  A.FK_TRANSACTION0ID AS TRX_CODE,
                  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
                  C.DESCRIPTION AS justific_descr,
                  NULL AS PRF_ACCOUNT_NUMBER,
                  0 AS PRF_ACC_CD,
                  REPLACE (A.FK_GLG_ACCOUNTACCO, '.', '')
                     AS external_glaccount,
                  fk_customercust_id AS cust_id,
                  0 AS gl_rule,
                  glacc_origin,
                  amount_ser_no,
                  trn_date,
                  fk_unitcode AS fk_unitcodetrxunit,
                  fk_usrcode,
                  TO_NUMBER (
                     SUBSTR (remarks, 17 + LENGTH (TRIM (trx_usr)), 8))
                     AS trx_sn,
                  TO_NUMBER (
                     SUBSTR (
                        remarks,
                        INSTR (remarks, '-', 1) + 1,
                        LENGTH (TRIM (remarks)) - (INSTR (remarks, '-', 1))))
                     AS tun_internal_sn,
                  remarks,
                  NULL AS account_number,
				  FK_GLG_JUSTIFYJUST,
				  a.line_num as line_num,
				  a.trn_snum as trn_snum
             FROM fxft_gli_interface a
             --, currency b, JUSTIFIC C
          --  WHERE     b.id_currency = a.fk_currencyid_curr
          --        AND a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC(+)
            LEFT JOIN CURRENCY B
                    ON (A.FK_CURRENCYID_CURR = B.ID_CURRENCY)
                  LEFT JOIN JUSTIFIC C
                    ON (A.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC)
           UNION ALL
           SELECT A.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
                  a.FK1UNITCODE AS FK1UNITCODE,
                  b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                  SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                     AS TRX_USR,
                  a.GL_TRN_DATE AS TRX_GL_TRN_DATE,
                  a.AMOUNT AS FC_AMOUNT,
                  A.FK_TERMINALTERMINA AS ENTRY_COMMENTS,
                  A.ENTRY_TYPE,
                  SUBSYSTEM,
                  0 AS ID_PRODUCT,
                  A.FK_TRANSACTION0ID AS TRX_CODE,
                  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
                  C.DESCRIPTION AS justific_descr,
                  NULL PRF_ACCOUNT_NUMBER,
                  NULL AS PRF_ACC_CD,
                  REPLACE (A.FK_GLG_ACCOUNTACCO, '.', '')
                     AS external_glaccount,
                  FK_CUSTOMERCODE AS cust_id,
                  0 AS gl_rule,
                  glacc_origin,
                  amount_ser_no,
                  trn_date,
                  fk_unitcode AS fk_unitcodetrxunit,
                  fk_usrcode,
                  TO_NUMBER (
                     SUBSTR (remarks, INSTR (remarks, '-', 1) - 8, 8))
                     AS trx_sn,
                  TO_NUMBER (
                     SUBSTR (
                        remarks,
                        INSTR (remarks, '-', 1) + 1,
                        LENGTH (TRIM (remarks)) - (INSTR (remarks, '-', 1))))
                     AS tun_internal_sn,
                  remarks,
                  TO_CHAR (account_number) AS account_number,
				  FK_GLG_JUSTIFYJUST,
				  a.line_num as line_num,
				  a.trn_snum as trn_snum
             FROM gli_interface a
             --, currency b, JUSTIFIC C
          --  WHERE     b.id_currency = a.fk_currencyid_curr
           --       AND a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC(+)
             LEFT JOIN CURRENCY B
                    ON (A.FK_CURRENCYID_CURR = B.ID_CURRENCY)
                  LEFT JOIN JUSTIFIC C
                    ON (A.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC)
           UNION ALL
           SELECT A.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
                  a.FK1UNITCODE AS FK1UNITCODE,
                  b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                  SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11) AS TRX_USR,
                  a.GL_TRN_DATE AS TRX_GL_TRN_DATE,
                  a.AMOUNT AS FC_AMOUNT,
                  A.FK_TERMINALTERMINA AS ENTRY_COMMENTS,
                  A.ENTRY_TYPE,
                  SUBSYSTEM,
                  0 AS ID_PRODUCT,
                  A.FK_TRANSACTION0ID AS TRX_CODE,
                  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
                  C.DESCRIPTION AS justific_descr,
                  NULL AS PRF_ACCOUNT_NUMBER,
                  0 AS PRF_ACC_CD,
                  REPLACE (A.FK_GLG_ACCOUNTACCO, '.', '')
                     AS external_glaccount,
                  FK_CUSTOMERCUST_ID AS cust_id,
                  0 AS gl_rule,
                  glacc_origin,
                  amount_ser_no,
                  trn_date,
                  fk_unitcode AS fk_unitcodetrxunit,
                  fk_usrcode,
                  TO_NUMBER (
                     SUBSTR (remarks, INSTR (remarks, '-', 1) - 8, 8))
                     AS trx_sn,
                  TO_NUMBER (
                     SUBSTR (
                        remarks,
                        INSTR (remarks, '-', 1) + 1,
                        LENGTH (TRIM (remarks)) - (INSTR (remarks, '-', 1))))
                     AS tun_internal_sn,
                  remarks,
                  TO_CHAR (account_number) AS account_number,
				  FK_GLG_JUSTIFYJUST,
				  a.line_num as line_num,
				  a.trn_snum as trn_snum
             FROM prf_gli_interface a
             --, currency b, JUSTIFIC C
          --  WHERE     b.id_currency = a.fk_currencyid_curr
          --        AND a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC(+)
            LEFT JOIN CURRENCY B
                    ON (A.FK_CURRENCYID_CURR = B.ID_CURRENCY)
                  LEFT JOIN JUSTIFIC C
                    ON (A.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC)
			UNION ALL
			SELECT
			g.FK_GLG_ACCOUNTACCO AS FK_GLG_ACCOUNTACCO,
            g.FK1UNITCODE AS FK1UNITCODE,
	        c.SHORT_DESCR AS CURRENCY_SHORT_DES,
			g.FK_USRCODE AS TRX_USR,
			g.GL_TRN_DATE AS TRX_GL_TRN_DATE,
			g.AMOUNT AS FC_AMOUNT,
			g.REMARKS AS ENTRY_COMMENTS,
			g.ENTRY_TYPE,
			g.SUBSYSTEM,
			0 AS ID_PRODUCT,
			0 AS TRX_CODE,
			'0' AS ID_JUSTIFIC,
			J.DESCR AS justific_descr,
			NULL AS PRF_ACCOUNT_NUMBER,
			0 AS PRF_ACC_CD,
			'' AS external_glaccount,
			0 AS cust_id,
			0 AS gl_rule,
			'1' as glacc_origin,
			1 as amount_ser_no,
			g.TRN_DATE,
			g.FK_UNITCODE AS fk_unitcodetrxunit,
			g.FK_USRCODE,
			g.TRN_SNUM AS trx_sn,
			g.LINE_NUM AS tun_internal_sn,
			g.COMMENT_REMARKS as remarks,
			NULL AS account_number,
			G.FK_GLG_JUSTIFYJUST,
		    g.line_num as line_num,
			g.trn_snum as trn_snum
			FROM GLG_FINAL_TRN g
     , CURRENCY c, GLG_JUSTIFY J
      , GENERIC_DETAIL P
       	WHERE g.SUBSYSTEM IN ('5', '05')
        AND c.ID_CURRENCY = g.FK_CURRENCYID_CURR
			AND J.JUSTIFY_ID = G.FK_GLG_JUSTIFYJUST
			AND P.FK_GENERIC_HEADPAR = 'GLPAR' AND P.SERIAL_NUM = 25 AND P.ENTRY_STATUS = '1'
			AND P.SHORT_DESCRIPTION = '1');

