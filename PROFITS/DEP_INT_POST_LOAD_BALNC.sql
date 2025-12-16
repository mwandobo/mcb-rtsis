CREATE PROCEDURE DEP_INT_POST_LOAD_BALNC
   LANGUAGE SQL
BEGIN
   -- 1 Find the total number of deposit accounts for Interest Posting
   --   Number of records at the INTEREST_RES_INFO table
   -- 2 Divide the number found by the execution number
   --   Read Deposit_parameters.PARALLEL_SESSIONS
   -- 3 Loop and calculate the account numbers for the load balancing.
   
   DECLARE l_dep_count decfloat ( 16 ) DEFAULT 0;
   DECLARE l_dep_count1 decfloat ( 16 ) DEFAULT 0;
   DECLARE l_dep_count2 decfloat ( 16 ) DEFAULT 0;
   DECLARE l_count decfloat ( 16 ) DEFAULT 0;
   DECLARE CNT decfloat ( 16 ) DEFAULT 0;
   DECLARE sn decfloat ( 16 ) DEFAULT 0;
   DECLARE l_account decfloat ( 16 ) ;
   DECLARE l_account_from decfloat ( 16 ) DEFAULT 0;
   DECLARE l_sessions decfloat ( 16 ) ;
   DECLARE l_program_id   CHAR ( 5 ) ;
   DECLARE l_deposit_type CHAR ( 1 ) ;
   DECLARE l_sn decfloat ( 16 ) ;
   DECLARE AT_END INT DEFAULT 0;
   
   DECLARE c_check_sessions CURSOR FOR
   SELECT NVL ( parallel_sessions , 1 ) FROM deposit_parameters;
   
   DECLARE c_account_number_fd CURSOR FOR
   SELECT 
      COUNT ( * )
   FROM 
      interest_res_info a
   WHERE 
      EXISTS
      ( 
         SELECT 
            1 
         FROM 
            deposit_Account b
         WHERE 
            A.ACC_SN = B.ACCOUNT_NUMBER
            AND 
            B.DEPOSIT_TYPE = '1' 
      ) ;
   
   DECLARE c_account_number_ov CURSOR FOR
   SELECT 
      COUNT ( * )
   FROM 
      interest_res_info a
   WHERE 
      EXISTS
      ( 
         SELECT 
            1 
         FROM 
            deposit_Account b
         WHERE 
            A.ACC_SN = B.ACCOUNT_NUMBER
            AND 
            B.DEPOSIT_TYPE = '5' 
      ) ;
   
   DECLARE c_find_account_no CURSOR FOR
   SELECT 
      a.acc_sn , 
      b.deposit_type
   FROM 
      interest_res_info a , 
      deposit_account   b
   WHERE 
      A.ACC_SN = b.account_number
   ORDER BY 
      1;
   
   DECLARE c_which_program CURSOR FOR
   SELECT 
      program_id
   FROM 
      dep_batch_param
   WHERE 
      program_id = l_program_id FOR UPDATE OF ACCOUNT_FROM , 
      ACCOUNT_TO;
   
   DECLARE CONTINUE HANDLER FOR NOT FOUND SET AT_END = 1;
   
   OPEN c_check_sessions;
   FETCH c_check_sessions INTO l_sessions;
    
   CLOSE c_check_sessions;
   
   OPEN c_account_number_fd;
   FETCH c_account_number_fd INTO l_dep_count1;
    
   CLOSE c_account_number_fd;
   
   OPEN c_account_number_ov;
   FETCH c_account_number_ov INTO l_dep_count2;
    
   CLOSE c_account_number_ov;
   
   -- An overdraft has a weight of 10
   SET l_dep_count = ( l_dep_count1 * 0.1 ) + l_dep_count2;
   
   IF l_sessions  = 0 THEN
      SET l_count = l_dep_count / 1;
   ELSE
      SET l_count = ROUND ( l_dep_count / l_sessions , 1 ) + 1;
   END IF;
   
   -- We need a loop to go through the number of executions and to split the accounts
   OPEN c_find_account_no;
   LOOP
      FETCH c_find_account_no INTO l_account , l_deposit_type;
      
      --   if c_find_account_no%notfound then
      --       exit;
      --   end if;
      IF AT_END = 1 THEN
         GOTO L2;
      END IF;
      
      IF l_deposit_type = '1' THEN
         SET CNT        = CNT + 0.1;
      ELSE
         SET CNT = CNT + 1;
      END IF;
      IF CNT              > L_count THEN
         SET cnt          = 0;
         SET sn           = sn +1;
         SET l_program_id = '7311' 
         || 
         SN;
         OPEN c_which_program;
         FETCH c_which_program INTO l_program_id;
          
         --         if c_which_program%found then
         IF AT_END = 0 THEN
            UPDATE 
               dep_batch_param
            SET account_from = l_account_from ,
               account_to    = l_account
            WHERE 
               CURRENT OF c_which_program;
          
         END IF;
         CLOSE c_which_program;
         SET cnt            = 0;
         SET l_account_from = l_account;
         COMMIT;
      END IF;
   END LOOP;
   CLOSE c_find_account_no;
   SET sn           = sn + 1;
   SET l_program_id = '7311' 
   || 
   SN;
   OPEN c_which_program;
   FETCH c_which_program INTO l_program_id;
    
   --   if c_which_program%found then
   IF AT_END = 0 THEN
      UPDATE 
         dep_batch_param
      SET account_from = l_account_from ,
         account_to    = 99999999999
      WHERE 
         CURRENT OF c_which_program;
    
   END IF;
   CLOSE c_which_program;
   COMMIT;
   -- Get the sn, check against the number of parallel sessions
   IF sn    <> l_sessions THEN
      SET sn = sn + 1;
      --  for l_sn in sn..l_sessions loop
      LOOP
         L1: SET l_program_id = '7311' 
         || 
         SN;
         OPEN c_which_program;
         FETCH c_which_program INTO l_program_id;
          
         --        if c_which_program%found then
         IF AT_END = 0 THEN
            UPDATE 
               dep_batch_param
            SET account_from = 0 ,
               account_to    = 0
            WHERE 
               CURRENT OF c_which_program;
          
         END IF;
         CLOSE c_which_program;
         SET sn = sn +1;
         IF sn <= l_sessions THEN
            GOTO L1;
         END IF;
      END LOOP;
   END IF;
   
   -- EXCEPTION
   --     WHEN OTHERS THEN
   --     RAISE_APPLICATION_ERROR(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
   L2: SET AT_END = 0;
END;

