create table W_DIM_GL_ACCOUNT
(
    ACCOUNT_ID    CHAR(21) not null
        constraint PK_W_DIM_GL_ACCOUNT
            primary key,
    ACCOUNT_LEVEL CHAR(1),
    DESCRIPTION   VARCHAR(60)
);

CREATE PROCEDURE W_DIM_GL_ACCOUNT ( )
  SPECIFIC SQL160620112633552
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_dim_gl_account a
USING      (SELECT account_id, level0 account_level, descr description
            FROM   glg_account) b
ON         (a.account_id = b.account_id)
WHEN NOT MATCHED
THEN
   INSERT     (account_id, account_level, description)
   VALUES     (b.account_id, b.account_level, b.description)
WHEN MATCHED
THEN
   UPDATE SET
      a.account_level = b.account_level, a.description = b.description;
END;

