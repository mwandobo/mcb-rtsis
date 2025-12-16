CREATE VIEW GL_DATE_PERIOD
(
   DATE_ID,
   PERIOD
)
AS
   SELECT "DATE_ID", "PERIOD"
     FROM (SELECT date_id, GL_PERIOD (date_id) period
             FROM calendar
           UNION
           SELECT date_id,'13-'|| ( cast(utilpkg.numtext(SUBSTR(GL_PERIOD(date_id), length(GL_PERIOD (date_id))-3, 4)) - 1 as integer) )
           FROM glg_entep_ctl, calendar
           WHERE     glg_entep_ctl.curr_fiscyear <> glg_entep_ctl.next_fiscyear
           AND EXTRACT (YEAR FROM date_id) = glg_entep_ctl.next_fiscyear)
          WHERE date_id > DATE '2010-01-01';

