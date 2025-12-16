create table EOM_GENERIC_DETAIL
(
    EOM_DATE             DATE,
    FK_GENERIC_HEADPAR   CHAR(5),
    SERIAL_NUM           INTEGER,
    TMSTAMP              DATE,
    ENTRY_STATUS         CHAR(1),
    PARAMETER_TYPE       CHAR(5),
    SHORT_DESCRIPTION    CHAR(10),
    LATIN_DESC           VARCHAR(40),
    DESCRIPTION          VARCHAR(40),
    HDR_DEFAULT_NUM      INTEGER,
    HDR_TMSTAMP          DATE,
    HDR_SYSTEM_PARAMETER VARCHAR(1),
    HDR_SHOW_FLAG        VARCHAR(1),
    HDR_PURPOSE_FLAG     VARCHAR(1),
    HDR_ENTRY_STATUS     VARCHAR(1),
    HDR_ABBREVIATION     VARCHAR(15),
    HDR_DESCRIPTION      VARCHAR(40)
);

create unique index PK_DETAIL
    on EOM_GENERIC_DETAIL (EOM_DATE, FK_GENERIC_HEADPAR, SERIAL_NUM);

CREATE PROCEDURE EOM_GENERIC_DETAIL ( )
  SPECIFIC SQL160620112636367
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_generic_detail
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_generic_detail (
               eom_date
              ,fk_generic_headpar
              ,serial_num
              ,tmstamp
              ,entry_status
              ,parameter_type
              ,short_description
              ,latin_desc
              ,description
              ,hdr_default_num
              ,hdr_tmstamp
              ,hdr_system_parameter
              ,hdr_show_flag
              ,hdr_purpose_flag
              ,hdr_entry_status
              ,hdr_abbreviation
              ,hdr_description)
   SELECT (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,d.fk_generic_headpar
         ,d.serial_num
         ,d.tmstamp
         ,d.entry_status
         ,d.parameter_type
         ,d.short_description
         ,d.latin_desc
         ,d.description
         ,h.default_num hdr_default_num
         ,h.tmstamp hdr_tmstamp
         ,h.system_parameter hdr_system_parameter
         ,h.show_flag hdr_show_flag
         ,h.purpose_flag hdr_purpose_flag
         ,h.entry_status hdr_entry_status
         ,h.abbreviation hdr_abbreviation
         ,h.description hdr_description
   FROM   generic_detail d
          LEFT JOIN generic_header h
             ON (h.parameter_type = d.fk_generic_headpar);
END;

