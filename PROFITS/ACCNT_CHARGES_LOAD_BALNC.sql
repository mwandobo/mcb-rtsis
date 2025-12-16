CREATE PROCEDURE ACCNT_CHARGES_LOAD_BALNC
   LANGUAGE SQL
   
   -- 1 Find the total number of LOAN_ACCOUNT PROCESS records to be processed
   --   Number of records at the LOAN_ACCNT_PROCESS table.
   -- 2 Divide the number found by the execution number
   --   Read Deposit_parameters.PARALLEL_SESSIONS
   -- 3 Loop and calculate the account numbers for the load balancing.

BEGIN
   DECLARE l_dep_count decfloat ( 16 ) DEFAULT 0;
   DECLARE l_count decfloat ( 16 ) DEFAULT 0;
   DECLARE CNT decfloat ( 16 ) DEFAULT 0;
   DECLARE sn decfloat ( 16 ) DEFAULT 0;
   DECLARE l_account decfloat ( 16 ) ;
   DECLARE l_account_from decfloat ( 16 ) DEFAULT 0;
   DECLARE l_sessions decfloat ( 16 ) ;
   DECLARE l_program_id CHAR ( 5 ) ;
   DECLARE l_sn decfloat ( 16 ) ;
   DECLARE AT_END INT DEFAULT 0;
   
   DECLARE c_check_sessions CURSOR FOR
   SELECT NVL ( parallel_sessions , 1 ) FROM deposit_parameters;
   
   DECLARE c_account_number CURSOR FOR
   SELECT COUNT ( * ) FROM loan_Accnt_process WHERE process_flg = '0';
   
   DECLARE c_find_account_no CURSOR FOR
   SELECT 
      account_ser_num
   FROM 
      loan_Accnt_process
   WHERE 
      process_flg = '0'
   ORDER BY 
      account_ser_num;
   
   DECLARE c_which_program CURSOR FOR
   SELECT 
      program_id
   FROM 
      batch_parameters
   WHERE 
      program_id = l_program_id FOR UPDATE OF NUMBER_FROM , 
      NUMBER_TO;
   
   DECLARE CONTINUE HANDLER FOR NOT FOUND SET AT_END = 1;
   UPDATE 
      loan_accnt_process a
   SET account_ser_num =
      ( 
         SELECT 
            account_ser_num
         FROM 
            profits_account B
         WHERE 
            A.ACC_SN = B.LNS_SN
            AND 
            A.ACC_TYPE = B.LNS_TYPE
            AND 
            A.ACC_UNIT = B.LNS_OPEN_UNIT 
      )
   WHERE 
      A.ACC_SN <> 0
      AND 
      A.ACC_TYPE <> 0
      AND 
      A.ACC_UNIT <> 0
      AND 
      A.PROCESS_FLG IN ( '0' , 
                        '2' ) ;
    
   COMMIT;
   --
   UPDATE 
      loan_Accnt_process a
   SET account_ser_num =
      ( 
         SELECT 
            account_ser_num
         FROM 
            profits_account B
         WHERE 
            A.ACC_SN = b.dep_acc_number
            AND 
            B.PRFT_SYSTEM = '3' 
      )
   WHERE 
      A.ACC_SN <> 0
      AND 
      A.ACC_TYPE = 0
      AND 
      A.PROCESS_FLG IN ( '0' , 
                        '2' ) ;
    
   COMMIT;
   
   OPEN c_check_sessions;
   FETCH c_check_sessions INTO l_sessions;
    
   CLOSE c_check_sessions;
   
   OPEN c_account_number;
   FETCH c_account_number INTO l_dep_count;
    
   CLOSE c_account_number;
   
   IF l_sessions  = 0 THEN
      SET l_count = l_dep_count / 1;
   ELSE
      SET l_count = ROUND ( l_dep_count / l_sessions , 0 ) + 1;
   END IF;
   
   -- We need a loop to go through the number of executions and to split the accounts
   OPEN c_find_account_no;
   LOOP
      FETCH c_find_account_no INTO l_account;
       
      IF at_end = 1 THEN
         GOTO exit_label;
      END IF;
      SET CNT             = CNT+1;
      IF CNT              = L_count THEN
         SET sn           = sn +1;
         SET l_program_id = 'C746' 
         || 
         SN;
         OPEN c_which_program;
         FETCH c_which_program INTO l_program_id;
          
         IF at_end = 0 THEN
            UPDATE 
               batch_parameters
            SET number_from = l_account_from ,
               number_to    = l_account
            WHERE 
               CURRENT OF c_which_program;
          
         END IF;
         CLOSE c_which_program;
         SET cnt            = 0;
         SET l_account_from = l_account +1;
         COMMIT;
      END IF;
   END LOOP;
   exit_label: CLOSE c_find_account_no;
   SET sn           = sn +1;
   SET l_program_id = 'C746' 
   || 
   SN;
   OPEN c_which_program;
   FETCH c_which_program INTO l_program_id;
    
   IF at_end = 0 THEN
      UPDATE 
         batch_parameters
      SET number_from = l_account_from ,
         number_to    = 99999999999
      WHERE 
         CURRENT OF c_which_program;
    
   END IF;
   CLOSE c_which_program;
   COMMIT;
   -- Get the sn, check against the number of parallel sessions
   IF sn    <> l_sessions THEN
      SET sn = sn +1;
      --    for l_sn in sn..l_sessions loop
      LOOP
         l1: SET l_program_id = 'C746' 
         || 
         SN;
         OPEN c_which_program;
         FETCH c_which_program INTO l_program_id;
          
         IF at_end = 0 THEN
            UPDATE 
               batch_parameters
            SET number_from = 9999999999999 ,
               number_to    = 9999999999999
            WHERE 
               CURRENT OF c_which_program;
          
         END IF;
         CLOSE c_which_program;
         SET sn = sn +1;
         IF sn <= l_sessions THEN
            GOTO l1;
         END IF;
      END LOOP;
   END IF;
   
   -- EXCEPTION
   --    WHEN OTHERS THEN
   --    RAISE_APPLICATION_ERROR(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
   --    SIGNAL SQLSTATE VALUE '20001', SET MESSAGE_TEXT = 'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM;
END;

