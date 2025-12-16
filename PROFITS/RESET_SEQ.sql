create procedure reset_seq(IN p_seq_name varchar(128))
language sql
begin
    declare l_val decfloat(16);

    execute immediate
    'select ' || p_seq_name || '.nextval from sysibm.sysdummy1 INTO l_val';

    execute immediate
    'alter sequence ' || p_seq_name || ' increment by -' || l_val ||
                                                          ' minvalue 0';

    execute immediate
    'select ' || p_seq_name || '.nextval from sysibm.sysdummy1 INTO l_val';

    execute immediate
    'alter sequence ' || p_seq_name || ' increment by 1 minvalue 0';
end;

