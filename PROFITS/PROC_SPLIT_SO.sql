CREATE PROCEDURE PROC_SPLIT_SO()
  LANGUAGE SQL
BEGIN
DECLARE interv   DECIMAL(10);
DECLARE maxim    DECIMAL(10);
    SELECT MAX(TP_SO_IDENTIFIER) / 5 INTO interv FROM HIST_SO_COMMITMENT WHERE ENTRY_STATUS = '0';
	SELECT MAX(TP_SO_IDENTIFIER)     INTO maxim  FROM HIST_SO_COMMITMENT WHERE ENTRY_STATUS = '0';
    update TPP_BATCH_PARAM set CP_AGREEMENT_FROM = 0           , CP_AGREEMENT_TO = interv*1  where program_id = '9104A';
    update TPP_BATCH_PARAM set CP_AGREEMENT_FROM = (interv*1)+1, CP_AGREEMENT_TO = interv*2  where program_id = '9104B';
	update TPP_BATCH_PARAM set CP_AGREEMENT_FROM = (interv*2)+1, CP_AGREEMENT_TO = interv*3  where program_id = '9104C';
	update TPP_BATCH_PARAM set CP_AGREEMENT_FROM = (interv*3)+1, CP_AGREEMENT_TO = interv*4  where program_id = '9104D';
	update TPP_BATCH_PARAM set CP_AGREEMENT_FROM = (interv*4)+1, CP_AGREEMENT_TO = maxim     where program_id = '9104E';
END;

