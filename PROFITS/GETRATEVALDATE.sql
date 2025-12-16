CREATE FUNCTION           "GETRATEVALDATE" 
( IN CurDate DATE
, IN PrvScale DATE
, IN PrvExpire DATE
, IN CurScale  DATE
, IN CurExpire DATE
, IN Floating  DECFLOAT(16)
, IN Curr DECFLOAT(16)
) RETURNS date 
LANGUAGE SQL
BEGIN
DECLARE ttt DATE;
select max(validity_date) into ttt  from int_scale where FK_INTERESTID_INTE= floating and fk_currencyid_curr= curr;
if PrvExpire>=CurDate then
   set ttt = PrvScale;
end if;
if CurExpire>=CurDate then
   set ttt = CurScale;
end if;
return ttt;
END;

