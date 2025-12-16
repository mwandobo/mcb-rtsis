CREATE FUNCTION GETRATECODE
(
   IN CurDate DATE,
   IN PrvFix DECFLOAT(16),
   IN PrvExpire DATE,
   IN CurFix DECFLOAT(16),
   IN CurExpire DATE,
   IN Floating DECFLOAT(16)
)
RETURNS DECFLOAT(16)
LANGUAGE SQL
BEGIN
   DECLARE t1 DECFLOAT(16);
   SET t1 = floating;
   if PrvExpire>=CurDate then
      SET t1=PrvFix;
   end if;
   if CurExpire>=CurDate then
      set t1=CurFix;
   end if;
return t1;
END;

