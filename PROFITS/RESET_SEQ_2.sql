CREATE PROCEDURE RESET_SEQ_2
(IN p_seq_name VARCHAR(128)) language SQL
BEGIN
   DECLARE l_val decfloat(16);
   EXECUTE immediate 'select ' ||
   p_seq_name                  ||
   '.nextval from sysibm.sysdummy1 INTO l_val';
   EXECUTE immediate 'alter sequence ' ||
   p_seq_name                          ||
   ' increment by -'                   ||
   l_val                               ||
   ' minvalue 0';
   EXECUTE immediate 'select ' ||
   p_seq_name                  ||
   '.nextval from sysibm.sysdummy1 INTO l_val';
   EXECUTE immediate 'alter sequence ' ||
   p_seq_name                          ||
   ' increment by 1 minvalue 0';
END;

