CREATE VIEW GLI_INTERBRANCH_OPENINGS
(
   FK_GLG_ACCOUNTACCO,
   CURRENCY_SHORT_DES,
   ID_CURRENCY,
   GL_TRN_DATE,
   TRX_USR,
   TRN_DATE,
   BALANCE,
   REMARKS,
   ID_JUSTIFIC,
   TRX_CODE,
   MAN_BALANCE,
   SUBSYSTEM
)
AS
     SELECT FK_GLG_ACCOUNTACCO,
            CURRENCY_SHORT_DES,
            ID_CURRENCY,
            GL_TRN_DATE,
            TRX_USR,
            trn_date,
            SUM (BALANCE) AS BALANCE,
            remarks,
			ID_JUSTIFIC,
			TRX_CODE,
			SUM (MAN_BALANCE) AS MAN_BALANCE,
			SUBSYSTEM
       FROM (  SELECT a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                      b.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                         AS TRX_USR,
                      SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END)
                         AS BALANCE,
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
					  A.FK_TRANSACTION0ID AS TRX_CODE,
					  nvl(SUM(CASE
                            WHEN a.ENTRY_TYPE = 1 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN NVL (a.AMOUNT, 0)
                            WHEN a.ENTRY_TYPE = 2 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN -NVL (a.AMOUNT, 0)
                         END),0) MAN_BALANCE,
						 A.SUBSYSTEM
                 FROM dep_gli_interface a left outer join JUSTIFIC C on a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC,
				 currency b, glg_entep_ctl g
                WHERE     b.id_currency = a.fk_currencyid_curr
                      AND g.fk_glg_interbranch = a.FK_GLG_ACCOUNTACCO
             GROUP BY a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR,
                      B.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks,
                              16,
                              LENGTH (TRIM (a.remarks)) - 16 - 11),
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST),
					  A.FK_TRANSACTION0ID,
					  A.SUBSYSTEM
               HAVING SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END) <> 0
             UNION ALL
               SELECT a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                      b.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                         AS TRX_USR,
                      SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END)
                         AS BALANCE,
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
					  A.FK_TRANSACTION0ID AS TRX_CODE,
					  nvl(SUM(CASE
                            WHEN a.ENTRY_TYPE = 1 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN NVL (a.AMOUNT, 0)
                            WHEN a.ENTRY_TYPE = 2 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN -NVL (a.AMOUNT, 0)
                         END),0) MAN_BALANCE,
						 A.SUBSYSTEM
                 FROM lns_gli_interface a left outer join JUSTIFIC C on a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC,
				 currency b, glg_entep_ctl g
                WHERE     b.id_currency = a.fk_currencyid_curr
                      AND g.fk_glg_interbranch = a.FK_GLG_ACCOUNTACCO
             GROUP BY a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR,
                      B.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks,
                              16,
                              LENGTH (TRIM (a.remarks)) - 16 - 11),
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST),
					  A.FK_TRANSACTION0ID,
					  A.SUBSYSTEM
               HAVING SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END) <> 0
             UNION ALL
               SELECT a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                      b.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                         AS TRX_USR,
                      SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END)
                         AS BALANCE,
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
					  A.FK_TRANSACTION0ID AS TRX_CODE,
					  nvl(SUM(CASE
                            WHEN a.ENTRY_TYPE = 1 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN NVL (a.AMOUNT, 0)
                            WHEN a.ENTRY_TYPE = 2 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN -NVL (a.AMOUNT, 0)
                         END),0) MAN_BALANCE,
						 A.SUBSYSTEM
                 FROM fxft_gli_interface a left outer join JUSTIFIC C on a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC,
				 currency b, glg_entep_ctl g
                WHERE     b.id_currency = a.fk_currencyid_curr
                      AND g.fk_glg_interbranch = a.FK_GLG_ACCOUNTACCO
             GROUP BY a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR,
                      B.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks,
                              16,
                              LENGTH (TRIM (a.remarks)) - 16 - 11),
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST),
					  A.FK_TRANSACTION0ID,
					  A.SUBSYSTEM
               HAVING SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END) <> 0
             UNION ALL
               SELECT a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                      b.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                         AS TRX_USR,
                      SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END)
                         AS BALANCE,
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
					  A.FK_TRANSACTION0ID AS TRX_CODE,
					  nvl(SUM(CASE
                            WHEN a.ENTRY_TYPE = 1 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN NVL (a.AMOUNT, 0)
                            WHEN a.ENTRY_TYPE = 2 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN -NVL (a.AMOUNT, 0)
                         END),0) MAN_BALANCE,
						 A.SUBSYSTEM
                 FROM gli_interface a left outer join JUSTIFIC C on a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC,
				 currency b, glg_entep_ctl g
                WHERE     b.id_currency = a.fk_currencyid_curr
                      AND g.fk_glg_interbranch = a.FK_GLG_ACCOUNTACCO
             GROUP BY a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR,
                      B.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks,
                              16,
                              LENGTH (TRIM (a.remarks)) - 16 - 11),
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST),
					  A.FK_TRANSACTION0ID,
					  A.SUBSYSTEM
               HAVING SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END) <> 0
             UNION ALL
               SELECT a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR AS CURRENCY_SHORT_DES,
                      b.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks, 16, LENGTH (TRIM (a.remarks)) - 16 - 11)
                         AS TRX_USR,
                      SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END)
                         AS BALANCE,
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST) AS ID_JUSTIFIC,
					  A.FK_TRANSACTION0ID AS TRX_CODE,
					  nvl(SUM(CASE
                            WHEN a.ENTRY_TYPE = 1 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN NVL (a.AMOUNT, 0)
                            WHEN a.ENTRY_TYPE = 2 and (a.subsystem = '05' or (a.subsystem = '12' and
                            a.FK_JUSTIFICID_JUST in (12500,12501))) THEN -NVL (a.AMOUNT, 0)
                         END),0) MAN_BALANCE,
						 A.SUBSYSTEM
                 FROM prf_gli_interface a left outer join JUSTIFIC C on a.FK_JUSTIFICID_JUST = C.ID_JUSTIFIC,
				 currency b, glg_entep_ctl g
                WHERE     b.id_currency = a.fk_currencyid_curr
                      AND g.fk_glg_interbranch = a.FK_GLG_ACCOUNTACCO
             GROUP BY a.FK_GLG_ACCOUNTACCO,
                      b.SHORT_DESCR,
                      B.ID_CURRENCY,
                      a.GL_TRN_DATE,
                      a.trn_date,
                      SUBSTR (a.remarks,
                              16,
                              LENGTH (TRIM (a.remarks)) - 16 - 11),
                      a.remarks,
					  TO_CHAR (A.FK_JUSTIFICID_JUST),
					  A.FK_TRANSACTION0ID,
					  A.SUBSYSTEM
               HAVING SUM (
                         CASE
                            WHEN a.ENTRY_TYPE = 1 THEN NVL (a.AMOUNT, 0)
                            ELSE -NVL (a.AMOUNT, 0)
                         END) <> 0)
   GROUP BY FK_GLG_ACCOUNTACCO,
            CURRENCY_SHORT_DES,
            ID_CURRENCY,
            GL_TRN_DATE,
            trn_date,
            TRX_USR,
            remarks,
			ID_JUSTIFIC,
			TRX_CODE,
			SUBSYSTEM
     HAVING SUM (BALANCE) <> 0;

