CREATE PROCEDURE RUN_GLACC
   LANGUAGE SQL
BEGIN
   DECLARE LGL          CHAR ( 21 ) ;
   DECLARE LPRFT_SYSTEM SMALLINT;
   DECLARE lserial_num  INTEGER;
   
   DECLARE C1 CURSOR FOR
   SELECT DISTINCT GL_ACCOUNT , 
      PRFT_SYSTEM
   FROM
      ( 
         SELECT DISTINCT GL_ACCOUNT , 
            PRFT_SYSTEM 
         FROM 
            HREP_74220 A 
         WHERE 
            PRFT_SYSTEM <> '88'
          
         UNION
          
         SELECT DISTINCT GL_ACCOUNT , 
            PRFT_SYSTEM 
         FROM 
            REP_74220 B 
         WHERE 
            PRFT_SYSTEM <> '88' 
      )
   WHERE 
      GL_ACCOUNT NOT IN 
      ( SELECT DESCRIPTION FROM GENERIC_DETAIL WHERE FK_GENERIC_HEADPAR = 'GLACC' 
      ) ;
    
   DECLARE C2 CURSOR FOR
   SELECT 
      NVL ( MAX ( SERIAL_NUM ) , 0 ) 
   FROM 
      GENERIC_DETAIL
   WHERE 
      FK_GENERIC_HEADPAR = 'GLACC';
    
   OPEN c2;
   FETCH c2 INTO LSERIAL_NUM;
    
   CLOSE C2;
   
   OPEN C1;
   LOOP
      FETCH C1 INTO LGL , LPRFT_SYSTEM;
       
      -- ** EXIT WHEN C1%NOTFOUND;
      SET LSERIAL_NUM = LSERIAL_NUM+1;
   BEGIN
      INSERT INTO GENERIC_DETAIL 
         ( 
            FK_GENERIC_HEADPAR , 
            SERIAL_NUM         , 
            TMSTAMP            , 
            ENTRY_STATUS       ,
            PARAMETER_TYPE     , 
            SHORT_DESCRIPTION  , 
            LATIN_DESC         , 
            DESCRIPTION 
         )
         VALUES 
         ( 
            'GLACC'     , 
            LSERIAL_NUM , 
            ( SELECT SYSDATE FROM SYSIBM.SYSDUMMY1 
            ) 
            , 
            '1'     , 
            'GLACC' , 
            'GLACC' , 
            NULL    ,
            LGL 
         ) ;
       
      INSERT INTO PAR_RELATION_DETAI 
         ( 
            FKGD_HAS_A_PRIMARY , 
            FKGD_HAS_A_SECONDA , 
            FKGH_HAS_A_PRIMARY , 
            FKGH_HAS_A_SECONDA ,
            FK_PAR_RELATIONCOD , 
            TMSTAMP            , 
            ENTRY_STATUS 
         )
         VALUES 
         ( 
            LPRFT_SYSTEM , 
            LSERIAL_NUM  , 
            'SUBSY'      , 
            'GLACC'      , 
            'SUBGLACC'   , 
            ( SELECT SYSDATE FROM SYSIBM.SYSDUMMY1 
            ) 
            , 
            '1' 
         ) ;
       
      -- EXCEPTION WHEN OTHERS THEN NULL;
   END;
END LOOP;
CLOSE C1;
COMMIT;
END
-- END;

