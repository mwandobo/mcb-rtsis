CREATE FUNCTION GET_DESIRED_GL_DATE (
    IN_DT	DATE,
    IN_DT_TYPE	VARCHAR(100) )
  RETURNS DATE
  SPECIFIC SQL160729110344513
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
begin
    declare v_month                  INTEGER;
    declare v_year                   INTEGER;
    declare v_date                   DATE;
    declare  dd                      DATE /*default to_date('01/01/0001','DD/MM/YYYY')*/ ;

    --FIRST DATE OF THE YEAR
    IF IN_DT_TYPE = 'FIRST DATE OF THE YEAR' THEN
     set   dd = TO_DATE('01/01/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --FIRST DATE OF CURRENT QUARTER
    IF IN_DT_TYPE = 'FIRST DATE OF CURRENT QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
         set   v_month = 4;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
         set   v_month = 7;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
         set   v_month = 10;
        end if;
      set  dd=TO_DATE('01/' || TO_CHAR(v_month) || '/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --FIRST DATE OF PREVIOUS QUARTER
    IF IN_DT_TYPE = 'FIRST DATE OF PREVIOUS QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 10;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
         set   v_year = v_year - 1;
          set  dd =TO_DATE('01/' || TO_CHAR(v_month) || '/' || v_year, 'DD/MM/YYYY') ;
            RETURN DD;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
          set  v_month = 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
          set  v_month = 4;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
         set   v_month = 7;
        end if;
       set dd=TO_DATE('01/' || TO_CHAR(v_month) || '/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --Last Day of MONTH
    IF IN_DT_TYPE = 'Last Day of MONTH' THEN
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = EXTRACT(MONTH FROM IN_DT)
        and EXTRACT(YEAR FROM A.DATE_ID) = EXTRACT(YEAR FROM IN_DT);
        RETURN DD;
    END IF;



    --LAST DATE OF PREVIOUS PERIOD
    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS PERIOD' THEN
        if EXTRACT(MONTH FROM IN_DT) = 1     then
          set  v_month = 12;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))- 1;
        else
         set  v_month =  EXTRACT(MONTH FROM IN_DT) - 1;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --LAST DATE OF PREVIOUS QUARTER
    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 12;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
         set   v_year = v_year - 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
           set v_month = 3;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
         set   v_month = 6;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
          set  v_month = 9;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --LAST DATE OF CURRENT QUARTER
    IF IN_DT_TYPE = 'LAST DATE OF CURRENT QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
          set  v_month = 3;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
          set  v_month = 6;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
          set  v_month = 9;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
          set  v_month = 12;
        end if;
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --NOLIMIT
    IF IN_DT_TYPE = 'NOLIMIT' THEN
      set  dd = DATE'2000-01-01';
        RETURN DD;
    END IF;

    --REPORT DATE
    IF IN_DT_TYPE = 'REPORT DATE' THEN
      set  dd = in_dt;
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 1 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 1 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 2 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 2 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 3 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 3 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 4 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 4 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;


    --LAST DATE SAME PERIOD PREVIOUS YEAR
    IF IN_DT_TYPE = 'LAST DATE SAME PERIOD PREVIOUS YEAR' THEN
      set  v_month = TO_NUMBER(EXTRACT(MONTH FROM IN_DT));
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))-1;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS YEAR' THEN
      set  v_month = 12;
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))-1;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

  IF SUBSTR(IN_DT_TYPE, 1, 7) = 'END OF ' THEN
  
    SET v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
  
    SET v_month = TO_NUMBER(TO_CHAR(TO_DATE(SUBSTR(IN_DT_TYPE, 8, LENGTH(IN_DT_TYPE) - 7), 'MONTH'), 'MM'));
    
    SELECT max(A.DATE_ID)
    INTO dd
    FROM calendar a
    WHERE EXTRACT(MONTH FROM A.DATE_ID) = v_month
    AND EXTRACT(YEAR FROM A.DATE_ID) = v_year;
    
    RETURN DD;
    
  END IF;

    
    IF IN_DT_TYPE = 'REPORT DATE MINUS 1 WORKING' THEN
        SELECT MAX(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID < in_dt
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;


      RETURN DD;
END
ALTER MODULE PROFITS.GL_PKG PUBLISH
	FUNCTION get_desired_gl_date(IN p_dt DATE, IN p_dt_type VARCHAR(40))
		RETURNS DATE
	BEGIN
  DECLARE		l_date DATE;
		SET l_date =
			CASE p_dt_type
				WHEN 'FIRST DATE OF THE YEAR'
				THEN
					TRUNC(p_dt, 'Y')
				WHEN 'FIRST DATE OF CURRENT QUARTER'
				THEN
					TRUNC(SYSDATE, 'Q')
				WHEN 'FIRST DATE OF PREVIOUS QUARTER'
				THEN
					ADD_MONTHS(TRUNC(p_dt, 'Q'), -3)
				WHEN 'Last Day of MONTH'
				THEN
					LAST_DAY(p_dt)
				WHEN 'LAST DATE OF PREVIOUS PERIOD'
				THEN
					LAST_DAY(ADD_MONTHS(p_dt, -1))
				WHEN 'LAST DATE OF PREVIOUS QUARTER'
				THEN
					LAST_DAY(ADD_MONTHS(TRUNC(p_dt, 'Q'), -1))
				WHEN 'LAST DATE OF CURRENT QUARTER'
				THEN
					LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'Q'), 2))
				WHEN 'NOLIMIT'
				THEN
					DATE '2000-01-01'
				WHEN 'REPORT DATE'
				THEN
					p_dt
				WHEN 'LAST DATE SAME PERIOD PREVIOUS YEAR'
				THEN
					LAST_DAY(ADD_MONTHS(p_dt, -12))
				WHEN 'LAST DATE OF PREVIOUS YEAR'
				THEN
					TO_DATE((EXTRACT(YEAR FROM p_dt) - 1) || '-12-31', 'YYYY-MM-DD')
				ELSE
					DATE '0001-01-01'
			END;
		IF		 SUBSTR(p_dt_type, 1, 7) = 'END OF '
			 AND SUBSTR(p_dt_type, 8) IN ('JANUARY'
																	 ,'FEBRUARY'
																	 ,'MARCH'
																	 ,'APRIL'
																	 ,'MAY'
																	 ,'JUNE'
																	 ,'JULY'
																	 ,'AUGUST'
																	 ,'SEPTEMBER'
																	 ,'OCTOBER'
																	 ,'NOVEMBER'
																	 ,'DECEMBER')
		THEN
			SET l_date =
				LAST_DAY(
					TO_DATE(
						'01-' || SUBSTR(p_dt_type, 8) || '-' || EXTRACT(YEAR FROM p_dt)
					 ,'DD-MONTH-YYYY'));
		END IF;
		IF p_dt_type LIKE 'REPORT DATE PLUS _ WORKING'
		THEN
			SELECT date_id
			INTO	 l_date
			FROM	 (SELECT ROW_NUMBER() OVER (ORDER BY date_id) - 1 days_added
										,date_id
							FROM	 calendar
							WHERE  date_id > p_dt AND holiday_ind <> '1')
			WHERE  days_added =
							 CASE p_dt_type
								 WHEN 'REPORT DATE PLUS 1 WORKING' THEN 1
								 WHEN 'REPORT DATE PLUS 2 WORKING' THEN 2
								 WHEN 'REPORT DATE PLUS 3 WORKING' THEN 3
								 WHEN 'REPORT DATE PLUS 4 WORKING' THEN 4
							 END;
		END IF;
		IF p_dt_type = 'REPORT DATE MINUS 1 WORKING'
		THEN
			SELECT MAX(date_id)
			INTO	 l_date
			FROM	 calendar
			WHERE  date_id < p_dt AND holiday_ind <> '1';
		END IF;
		RETURN l_date;
	END;

CREATE FUNCTION GET_DESIRED_GL_DATE (
    IN_DT	DATE,
    IN_DT_TYPE	VARCHAR(100) )
  RETURNS DATE
  SPECIFIC SQL160729110344513
  NOT SECURED
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  READS SQL DATA
  INHERIT SPECIAL REGISTERS
begin
    declare v_month                  INTEGER;
    declare v_year                   INTEGER;
    declare v_date                   DATE;
    declare  dd                      DATE /*default to_date('01/01/0001','DD/MM/YYYY')*/ ;

    --FIRST DATE OF THE YEAR
    IF IN_DT_TYPE = 'FIRST DATE OF THE YEAR' THEN
     set   dd = TO_DATE('01/01/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --FIRST DATE OF CURRENT QUARTER
    IF IN_DT_TYPE = 'FIRST DATE OF CURRENT QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
         set   v_month = 4;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
         set   v_month = 7;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
         set   v_month = 10;
        end if;
      set  dd=TO_DATE('01/' || TO_CHAR(v_month) || '/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --FIRST DATE OF PREVIOUS QUARTER
    IF IN_DT_TYPE = 'FIRST DATE OF PREVIOUS QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 10;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
         set   v_year = v_year - 1;
          set  dd =TO_DATE('01/' || TO_CHAR(v_month) || '/' || v_year, 'DD/MM/YYYY') ;
            RETURN DD;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
          set  v_month = 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
          set  v_month = 4;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
         set   v_month = 7;
        end if;
       set dd=TO_DATE('01/' || TO_CHAR(v_month) || '/' || TO_CHAR(EXTRACT(YEAR FROM IN_DT)), 'DD/MM/YYYY') ;
        RETURN DD;
    END IF;

    --Last Day of MONTH
    IF IN_DT_TYPE = 'Last Day of MONTH' THEN
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = EXTRACT(MONTH FROM IN_DT)
        and EXTRACT(YEAR FROM A.DATE_ID) = EXTRACT(YEAR FROM IN_DT);
        RETURN DD;
    END IF;



    --LAST DATE OF PREVIOUS PERIOD
    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS PERIOD' THEN
        if EXTRACT(MONTH FROM IN_DT) = 1     then
          set  v_month = 12;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))- 1;
        else
         set  v_month =  EXTRACT(MONTH FROM IN_DT) - 1;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --LAST DATE OF PREVIOUS QUARTER
    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
         set   v_month = 12;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
         set   v_year = v_year - 1;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
           set v_month = 3;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
         set   v_month = 6;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
          set  v_month = 9;
          set  v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        end if;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --LAST DATE OF CURRENT QUARTER
    IF IN_DT_TYPE = 'LAST DATE OF CURRENT QUARTER' THEN
        if EXTRACT(MONTH FROM IN_DT) in (1, 2, 3)     then
          set  v_month = 3;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (4, 5, 6)     then
          set  v_month = 6;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (7, 8, 9)     then
          set  v_month = 9;
        end if;
        if EXTRACT(MONTH FROM IN_DT) in (10, 11, 12)     then
          set  v_month = 12;
        end if;
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    --NOLIMIT
    IF IN_DT_TYPE = 'NOLIMIT' THEN
      set  dd = DATE'2000-01-01';
        RETURN DD;
    END IF;

    --REPORT DATE
    IF IN_DT_TYPE = 'REPORT DATE' THEN
      set  dd = in_dt;
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 1 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 1 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 2 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 2 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 3 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 3 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;
    --REPORT DATE PLUS 4 WORKING
    IF IN_DT_TYPE = 'REPORT DATE PLUS 4 WORKING' THEN
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > in_dt
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO V_DATE
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        SELECT MIN(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID > V_DATE
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;


    --LAST DATE SAME PERIOD PREVIOUS YEAR
    IF IN_DT_TYPE = 'LAST DATE SAME PERIOD PREVIOUS YEAR' THEN
      set  v_month = TO_NUMBER(EXTRACT(MONTH FROM IN_DT));
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))-1;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

    IF IN_DT_TYPE = 'LAST DATE OF PREVIOUS YEAR' THEN
      set  v_month = 12;
       set v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT))-1;
        select max(A.DATE_ID)
        INTO dd
        from calendar a
        where EXTRACT(MONTH FROM A.DATE_ID) = v_month
        and EXTRACT(YEAR FROM A.DATE_ID) = v_year;
        RETURN DD;
    END IF;

  IF SUBSTR(IN_DT_TYPE, 1, 7) = 'END OF ' THEN
  
    SET v_year = TO_NUMBER(EXTRACT(YEAR FROM IN_DT));
  
    SET v_month = TO_NUMBER(TO_CHAR(TO_DATE(SUBSTR(IN_DT_TYPE, 8, LENGTH(IN_DT_TYPE) - 7), 'MONTH'), 'MM'));
    
    SELECT max(A.DATE_ID)
    INTO dd
    FROM calendar a
    WHERE EXTRACT(MONTH FROM A.DATE_ID) = v_month
    AND EXTRACT(YEAR FROM A.DATE_ID) = v_year;
    
    RETURN DD;
    
  END IF;

    
    IF IN_DT_TYPE = 'REPORT DATE MINUS 1 WORKING' THEN
        SELECT MAX(DATE_ID)
        INTO DD
        FROM CALENDAR
        Where DATE_ID < in_dt
            AND HOLIDAY_IND <> '1';
        RETURN DD;
    END IF;


      RETURN DD;
END
ALTER MODULE PROFITS.GL_PKG PUBLISH
	FUNCTION get_desired_gl_date(IN p_dt DATE, IN p_dt_type VARCHAR(40))
		RETURNS DATE
	BEGIN
  DECLARE		l_date DATE;
		SET l_date =
			CASE p_dt_type
				WHEN 'FIRST DATE OF THE YEAR'
				THEN
					TRUNC(p_dt, 'Y')
				WHEN 'FIRST DATE OF CURRENT QUARTER'
				THEN
					TRUNC(SYSDATE, 'Q')
				WHEN 'FIRST DATE OF PREVIOUS QUARTER'
				THEN
					ADD_MONTHS(TRUNC(p_dt, 'Q'), -3)
				WHEN 'Last Day of MONTH'
				THEN
					LAST_DAY(p_dt)
				WHEN 'LAST DATE OF PREVIOUS PERIOD'
				THEN
					LAST_DAY(ADD_MONTHS(p_dt, -1))
				WHEN 'LAST DATE OF PREVIOUS QUARTER'
				THEN
					LAST_DAY(ADD_MONTHS(TRUNC(p_dt, 'Q'), -1))
				WHEN 'LAST DATE OF CURRENT QUARTER'
				THEN
					LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'Q'), 2))
				WHEN 'NOLIMIT'
				THEN
					DATE '2000-01-01'
				WHEN 'REPORT DATE'
				THEN
					p_dt
				WHEN 'LAST DATE SAME PERIOD PREVIOUS YEAR'
				THEN
					LAST_DAY(ADD_MONTHS(p_dt, -12))
				WHEN 'LAST DATE OF PREVIOUS YEAR'
				THEN
					TO_DATE((EXTRACT(YEAR FROM p_dt) - 1) || '-12-31', 'YYYY-MM-DD')
				ELSE
					DATE '0001-01-01'
			END;
		IF		 SUBSTR(p_dt_type, 1, 7) = 'END OF '
			 AND SUBSTR(p_dt_type, 8) IN ('JANUARY'
																	 ,'FEBRUARY'
																	 ,'MARCH'
																	 ,'APRIL'
																	 ,'MAY'
																	 ,'JUNE'
																	 ,'JULY'
																	 ,'AUGUST'
																	 ,'SEPTEMBER'
																	 ,'OCTOBER'
																	 ,'NOVEMBER'
																	 ,'DECEMBER')
		THEN
			SET l_date =
				LAST_DAY(
					TO_DATE(
						'01-' || SUBSTR(p_dt_type, 8) || '-' || EXTRACT(YEAR FROM p_dt)
					 ,'DD-MONTH-YYYY'));
		END IF;
		IF p_dt_type LIKE 'REPORT DATE PLUS _ WORKING'
		THEN
			SELECT date_id
			INTO	 l_date
			FROM	 (SELECT ROW_NUMBER() OVER (ORDER BY date_id) - 1 days_added
										,date_id
							FROM	 calendar
							WHERE  date_id > p_dt AND holiday_ind <> '1')
			WHERE  days_added =
							 CASE p_dt_type
								 WHEN 'REPORT DATE PLUS 1 WORKING' THEN 1
								 WHEN 'REPORT DATE PLUS 2 WORKING' THEN 2
								 WHEN 'REPORT DATE PLUS 3 WORKING' THEN 3
								 WHEN 'REPORT DATE PLUS 4 WORKING' THEN 4
							 END;
		END IF;
		IF p_dt_type = 'REPORT DATE MINUS 1 WORKING'
		THEN
			SELECT MAX(date_id)
			INTO	 l_date
			FROM	 calendar
			WHERE  date_id < p_dt AND holiday_ind <> '1';
		END IF;
		RETURN l_date;
	END;

