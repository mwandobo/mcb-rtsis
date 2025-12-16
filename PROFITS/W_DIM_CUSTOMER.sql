create table W_DIM_CUSTOMER
(
    CUSTOMER_KEY                   DECIMAL(11),
    CUST_ID                        INTEGER,
    ROW_START_DATE                 DATE,
    ROW_END_DATE                   DATE,
    ROW_CURRENT_FLAG               SMALLINT,
    C_DIGIT                        SMALLINT,
    DEL_CLEANESS                   SMALLINT,
    DEL_CHILDREN_ABOVE18           SMALLINT,
    NUM_OF_CHILDREN                SMALLINT,
    FAMILY_MEMBERS                 SMALLINT,
    DEL_FK_BRANCH_PORTFPOR         INTEGER,
    DEL_FK_BRANCH_PORTFBRA         INTEGER,
    FKUNIT_BELONGS                 INTEGER,
    FKUNIT_IS_MONITORE             INTEGER,
    DEL_FKCURR_HAS_AS_LIMI         INTEGER,
    FKCURR_THINKS_IN               INTEGER,
    DEL_FKUNIT_IS_SERVICED         INTEGER,
    DEL_FK_DISTR_CHANNEID          INTEGER,
    DEL_FK_BISS_CODE               INTEGER,
    DEL_SELF_NUM                   INTEGER,
    DEL_OLD_CUST_ID                DECIMAL(10),
    DEL_LIMIT                      DECIMAL(15, 2),
    DEL_CORRESPONDENT_LIMI         DECIMAL(15, 2),
    FIN_RANGE                      DECIMAL(15, 2),
    CERTIFIC_DATE                  DATE,
    SEPA_AGR_DT                    DATE,
    STATUS_DATE                    DATE,
    FIN_RANGE_DT                   DATE,
    TMSTAMP                        DATE,
    DATE_OF_BIRTH                  DATE,
    EXPIRE_DATE                    DATE,
    DOC_EXPIRE_DATE                DATE,
    CUSTOMER_BEGIN_DAT             DATE,
    LEGAL_EXPIRE_DATE              DATE,
    EMPLOYEMENT_START              DATE,
    LAST_UPDATE                    DATE,
    CERTIFIC_CUST                  CHAR(1),
    SEPA_AGR_FLG                   CHAR(1),
    NON_RESIDENT_FOR_R             CHAR(1),
    FAX_INDICATOR                  CHAR(1),
    MAJOR_BENEFICIARY              CHAR(1),
    BUSINESS_IND                   CHAR(1),
    MAIL_IND                       CHAR(1),
    DEL_SUN_NONWORK                CHAR(1),
    DEL_SAT_NONWORK                CHAR(1),
    ENTRY_STATUS                   CHAR(1),
    CUST_TYPE                      CHAR(1),
    DEL_VIP_IND                    CHAR(1),
    DEL_BLACKLISTED_IND            CHAR(1),
    NON_REGISTERED                 CHAR(1),
    CUST_STATUS                    CHAR(1),
    SEX                            CHAR(1),
    NON_RESIDENT                   CHAR(1),
    NO_AFM                         CHAR(1),
    DEL_TELEX_CONNECTION           CHAR(1),
    DEL_SWIFT_CONNECTION_I         CHAR(1),
    DEL_NOSTRO_ACCOUNT_IND         CHAR(1),
    DEL_VOSTRO_ACCOUNT_IND         CHAR(1),
    PROHIBIT_WITHDRAW              CHAR(1),
    DEL_NON_PROFIT                 CHAR(1),
    DEL_CONSOLID_STATM_FLG         CHAR(1),
    PENSIONER_IND                  CHAR(1),
    INSTITUTE_INV_IND              CHAR(1),
    SELF_INDICATOR                 CHAR(1),
    DEL_SEGM_FLAGS                 CHAR(5),
    TITLE                          CHAR(6),
    DEL_SPM_NUMBER                 CHAR(7),
    FK_CUST_BANKEMPID              CHAR(8),
    FK_BANKEMPLOYEEID              CHAR(8),
    FK0BANKEMPLOYEEID              CHAR(8),
    FK_USRCODE                     CHAR(8),
    CHAMBER_ID                     CHAR(10),
    DEL_DAI_NUMBER                 CHAR(12),
    TELEX                          CHAR(15),
    SHORT_NAME                     CHAR(15),
    LATIN_FIRSTNAME                CHAR(20),
    BIRTHPLACE                     CHAR(20),
    FIRST_NAME                     CHAR(20),
    DEL_FK_GLG_ACCOUNTACCO         CHAR(21),
    INCOMPLETE_U_COMNT             CHAR(30),
    ALERT_MSG                      CHAR(30),
    FATHER_SURNAME                 CHAR(40),
    SELF_NAME                      CHAR(40),
    SECOND_SURNAME                 CHAR(40),
    EMPLOYER                       CHAR(40),
    EMPLOYER_ADDRESS               CHAR(40),
    ATTRACTION_PERSON              CHAR(50),
    E_MAIL                         CHAR(64),
    SURNAME                        CHAR(70),
    LATIN_SURNAME                  CHAR(70),
    DEL_CLEANESS_COMMENTS          CHAR(80),
    PROMOCODE                      VARCHAR(6),
    DEL_SWIFT_ADDRESS              VARCHAR(12),
    MOBILE_TEL                     VARCHAR(15),
    TELEPHONE_1                    VARCHAR(15),
    MIDDLE_NAME                    VARCHAR(15),
    FATHER_NAME                    VARCHAR(20),
    MOTHER_NAME                    VARCHAR(20),
    SPOUSE_NAME                    VARCHAR(20),
    CHAMBER_COMMENTS               VARCHAR(30),
    MOTHER_SURNAME                 VARCHAR(40),
    DEL_ATTRACTION_DETAILS         VARCHAR(40),
    MARKETING_REMINDER             VARCHAR(80),
    INTERNET_ADDRESS               VARCHAR(100),
    ENTRY_COMMENTS                 VARCHAR(254),
    DEL_FICLI_DESC                 CHAR(42),
    DEL_FICLI_CODE                 INTEGER,
    NOT_RES_BOP                    CHAR(1),
    DEL_REF_B_TRX_USR_SN           INTEGER,
    DEL_REF_D_TRX_USR_SN           INTEGER,
    DEL_REF_TRX_USR                CHAR(8),
    DEL_REF_TRX_DATE               DATE,
    DEL_REF_TRX_UNIT               INTEGER,
    DEL_REF_DEP_ACC                DECIMAL(11),
    DEL_REF_CUSTID                 INTEGER,
    CITY_OF_BIRTH                  CHAR(20),
    DEL_REPR_PHONE                 CHAR(15),
    DEL_REPR_SURNAME               CHAR(70),
    DEL_REPR_FIRSTNAME             CHAR(20),
    DEL_IBAN_ACCOUNT               CHAR(27),
    DEL_BASEL_STATUS               CHAR(25),
    DEL_BASEL_DESCRIPTION          CHAR(90),
    AML_STATUS                     CHAR(1),
    TURNOVER_AMN                   DECIMAL(15, 2),
    DEL_LOANS_AMN                  DECIMAL(15, 2),
    DEL_PROFIT_AMN                 DECIMAL(15, 2),
    DEL_PROFITABILITY_AMN          DECIMAL(15, 2),
    DEL_SALARY_AMN                 DECIMAL(15, 2),
    DEL_ENABLE_FOR_24C             CHAR(1),
    CUST_OPEN_DATE                 DATE,
    NO_OF_BUSINESSES               INTEGER,
    DEL_OWNERSHIP_INDICATION       CHAR(1),
    DEL_CONTRACT_EXPIRY_DATE       DATE,
    DEL_CONTRACT                   CHAR(1),
    DEL_MOBILE_TEL2                VARCHAR(15),
    DEL_E_MAIL2                    VARCHAR(64),
    DEL_AFM_NO                     CHAR(20),
    DEL_ID_NO                      CHAR(20),
    DEL_FKGD_HAS_TYPE              INTEGER,
    DEL_OTHER_ID_DESC              VARCHAR(40),
    DEL_AFM_SERIAL_NO              SMALLINT,
    DEL_ID_ISSUE_DATE              DATE,
    DEL_ID_EXPIRY_DATE             DATE,
    DEL_ID_SERIAL_NO               SMALLINT,
    DEL_ID_COUNTRY_SERIAL_NUM      INTEGER,
    DEL_ID_COUNTRY_FK_GENERIC_HEAD CHAR(5),
    DEL_ID_COUNTRY_DESCRIPTION     VARCHAR(40),
    DEL_FKGD_HAS_COUNTRY           INTEGER,
    DEL_FKGH_HAS_COUNTRY           CHAR(5),
    DEL_C_COUNTRY                  VARCHAR(40),
    DEL_CUST_ADVANCES_CLASSIF_DATE DATE,
    DEL_CUST_ADVANCES_DESCRIPTION  VARCHAR(40),
    DEL_TAX_OFFICE_NAME            VARCHAR(20),
    DEL_FAX_NO                     CHAR(15),
    DEL_C_CITY                     CHAR(30),
    DEL_C_ZIP_CODE                 CHAR(10),
    DEL_C_TELEPHONE                CHAR(15),
    DEL_C_ADDRESS_1                VARCHAR(40),
    DEL_C_ADDRESS_2                VARCHAR(40),
    DEL_C_REGION                   VARCHAR(20),
    DEL_W_FAX_NO                   CHAR(15),
    DEL_W_CITY                     CHAR(30),
    DEL_W_ZIP_CODE                 CHAR(10),
    DEL_W_TELEPHONE                CHAR(15),
    DEL_W_ADDRESS_1                VARCHAR(40),
    DEL_W_ADDRESS_2                VARCHAR(40),
    DEL_W_REGION                   VARCHAR(20),
    DEL_W_COUNTRY                  VARCHAR(40),
    DEL_CLASSIF_DATE               DATE,
    NAME_STANDARD                  VARCHAR(107),
    BANKEMPLOYEE_NAME              VARCHAR(41),
    ADDRESS                        VARCHAR(245),
    TELEPHONE                      VARCHAR(15),
    NATIONAL_DESCRIPTION           VARCHAR(40),
    ID_NO                          CHAR(20),
    ID_ISSUE_AUTHORITY             CHAR(30),
    TAX_REGISTRATION_NO            VARCHAR(20),
    CITY                           VARCHAR(30),
    CUST_TYPE_IND                  VARCHAR(12),
    PHOTO_FLAG                     VARCHAR(8),
    SIGNATURE_FLAG                 VARCHAR(12),
    BANKEMPLOYEE_ID                VARCHAR(8)
);

create unique index W_DIM_CUSTOMER_IDX2
    on W_DIM_CUSTOMER (CUST_ID);

create unique index W_DIM_CUSTOMER_PK
    on W_DIM_CUSTOMER (CUSTOMER_KEY);

CREATE PROCEDURE W_DIM_CUSTOMER ( )
  SPECIFIC SQL160620112633348
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dim_customer
SET    row_end_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (cust_id) IN (SELECT t.cust_id
                         FROM   w_dim_customer t
                                INNER JOIN (SELECT cust_id
                                                  ,c_digit
                                                  ,num_of_children
                                                  ,family_members
                                                  ,fkunit_belongs
                                                  ,fkunit_is_monitore
                                                  ,fkcurr_thinks_in
                                                  ,fin_range
                                                  ,certific_date
                                                  ,sepa_agr_dt
                                                  ,status_date
                                                  ,fin_range_dt
                                                  ,tmstamp
                                                  ,date_of_birth
                                                  ,expire_date
                                                  ,doc_expire_date
                                                  ,customer_begin_dat
                                                  ,legal_expire_date
                                                  ,employement_start
                                                  ,last_update
                                                  ,certific_cust
                                                  ,sepa_agr_flg
                                                  ,non_resident_for_r
                                                  ,fax_indicator
                                                  ,major_beneficiary
                                                  ,business_ind
                                                  ,mail_ind
                                                  ,entry_status
                                                  ,cust_type
                                                  ,non_registered
                                                  ,cust_status
                                                  ,sex
                                                  ,non_resident
                                                  ,no_afm
                                                  ,prohibit_withdraw
                                                  ,pensioner_ind
                                                  ,institute_inv_ind
                                                  ,self_indicator
                                                  ,title
                                                  ,fk_cust_bankempid
                                                  ,fk_bankemployeeid
                                                  ,fk0bankemployeeid
                                                  ,fk_usrcode
                                                  ,chamber_id
                                                  ,telex
                                                  ,short_name
                                                  ,latin_firstname
                                                  ,birthplace
                                                  ,first_name
                                                  ,incomplete_u_comnt
                                                  ,alert_msg
                                                  ,father_surname
                                                  ,self_name
                                                  ,second_surname
                                                  ,employer
                                                  ,employer_address
                                                  ,attraction_person
                                                  ,e_mail
                                                 ,surname
                                                  ,latin_surname
                                                  ,promocode
                                                  ,mobile_tel
                                                  ,telephone_1
                                                  ,middle_name
                                                  ,father_name
                                                  ,mother_name
                                                  ,spouse_name
                                                  ,chamber_comments
                                                  ,mother_surname
                                                  ,marketing_reminder
                                                  ,internet_address
                                                  ,entry_comments
                                                  ,not_res_bop
                                                  ,city_of_birth
                                                  ,aml_status
                                                  ,turnover_amn
                                                  ,cust_open_date
                                                  ,no_of_businesses
                                                  ,name_standard
                                                  ,bankemployee_name
                                                  ,address
                                                  ,telephone
                                                  ,id_no
                                                  ,id_issue_authority
                                                  ,national_description
                                                  ,tax_registration_no
                                                  ,city
                                                  ,photo_flag
                                                  ,signature_flag
                                                  ,cust_type_ind
                                                  ,bankemployee_id
                                            FROM   w_dim_customer
                                            WHERE  row_current_flag = 1
                                            MINUS
                                            SELECT cust_id
                                                  ,c_digit
                                                  ,num_of_children
                                                  ,family_members
                                                  ,fkunit_belongs
                                                  ,fkunit_is_monitore
                                                  ,fkcurr_thinks_in
                                                  ,fin_range
                                                  ,certific_date
                                                  ,sepa_agr_dt
                                                  ,status_date
                                                  ,fin_range_dt
                                                  ,tmstamp
                                                  ,date_of_birth
                                                  ,expire_date
                                                  ,doc_expire_date
                                                  ,customer_begin_dat
                                                  ,legal_expire_date
                                                  ,employement_start
                                                  ,last_update
                                                  ,certific_cust
                                                  ,sepa_agr_flg
                                                  ,non_resident_for_r
                                                  ,fax_indicator
                                                  ,major_beneficiary
                                                  ,business_ind
                                                  ,mail_ind
                                                  ,entry_status
                                                  ,cust_type
                                                  ,non_registered
                                                  ,cust_status
                                                  ,sex
                                                  ,non_resident
                                                  ,no_afm
                                                  ,prohibit_withdraw
                                                  ,pensioner_ind
                                                  ,institute_inv_ind
                                                  ,self_indicator
                                                  ,title
                                                  ,fk_cust_bankempid
                                                  ,fk_bankemployeeid
                                                  ,fk0bankemployeeid
                                                  ,fk_usrcode
                                                  ,chamber_id
                                                  ,telex
                                                  ,short_name
                                                  ,latin_firstname
                                                  ,birthplace
                                                  ,first_name
                                                  ,incomplete_u_comnt
                                                  ,alert_msg
                                                  ,father_surname
                                                  ,self_name
                                                 ,second_surname
                                                  ,employer
                                                  ,employer_address
                                                  ,attraction_person
                                                  ,e_mail
                                                  ,surname
                                                  ,latin_surname
                                                  ,promocode
                                                  ,mobile_tel
                                                  ,telephone_1
                                                  ,middle_name
                                                  ,father_name
                                                  ,mother_name
                                                  ,spouse_name
                                                  ,chamber_comments
                                                  ,mother_surname
                                                  ,marketing_reminder
                                                  ,internet_address
                                                  ,entry_comments
                                                  ,not_res_bop
                                                  ,city_of_birth
                                                  ,aml_status
                                                  ,turnover_amn
                                                  ,cust_open_date
                                                  ,no_of_businesses
                                                  ,name_standard
                                                  ,bankemployee_name
                                                  ,address
                                                  ,telephone
                                                  ,id_no
                                                  ,id_issue_authority
                                                  ,national_description
                                                  ,tax_registration_no
                                                  ,city
                                                  ,photo_flag
                                                  ,signature_flag
                                                  ,cust_type_ind
                                                  ,bankemployee_id
                                            FROM   w_stg_customer) s
                                   ON (t.cust_id = s.cust_id));
INSERT INTO w_dim_customer (
               customer_key
              ,cust_id
              ,row_start_date
              ,row_end_date
              ,row_current_flag
              ,c_digit
              ,num_of_children
              ,family_members
              ,fkunit_belongs
              ,fkunit_is_monitore
              ,fkcurr_thinks_in
              ,fin_range
              ,certific_date
              ,sepa_agr_dt
              ,status_date
              ,fin_range_dt
              ,tmstamp
              ,date_of_birth
              ,expire_date
              ,doc_expire_date
              ,customer_begin_dat
              ,legal_expire_date
              ,employement_start
              ,last_update
              ,certific_cust
              ,sepa_agr_flg
              ,non_resident_for_r
              ,fax_indicator
              ,major_beneficiary
              ,business_ind
              ,mail_ind
              ,entry_status
              ,cust_type
              ,non_registered
              ,cust_status
              ,sex
              ,non_resident
              ,no_afm
              ,prohibit_withdraw
              ,pensioner_ind
              ,institute_inv_ind
              ,self_indicator
              ,title
              ,fk_cust_bankempid
              ,fk_bankemployeeid
              ,fk0bankemployeeid
              ,fk_usrcode
              ,chamber_id
              ,telex
              ,short_name
              ,latin_firstname
              ,birthplace
              ,first_name
              ,incomplete_u_comnt
              ,alert_msg
              ,father_surname
              ,self_name
              ,second_surname
              ,employer
              ,employer_address
              ,attraction_person
              ,e_mail
              ,surname
              ,latin_surname
              ,promocode
              ,mobile_tel
              ,telephone_1
              ,middle_name
              ,father_name
              ,mother_name
              ,spouse_name
              ,chamber_comments
              ,mother_surname
              ,marketing_reminder
              ,internet_address
              ,entry_comments
              ,not_res_bop
              ,city_of_birth
              ,aml_status
              ,turnover_amn
              ,cust_open_date
              ,no_of_businesses
              ,name_standard
              ,bankemployee_name
              ,address
              ,telephone
              ,id_no
              ,id_issue_authority
              ,national_description
              ,tax_registration_no
              ,city
              ,photo_flag
              ,signature_flag
              ,cust_type_ind
              ,bankemployee_id)
   SELECT   (SELECT NVL (MAX (customer_key), 0) FROM w_dim_customer)
          + ROW_NUMBER () OVER (ORDER BY cust_id)
         ,cust_id
         , (SELECT scheduled_date FROM bank_parameters) row_start_date
         ,DATE '9999-12-31' row_end_date
         ,1 row_current_flag
         ,c_digit
         ,num_of_children
         ,family_members
         ,fkunit_belongs
         ,fkunit_is_monitore
         ,fkcurr_thinks_in
         ,fin_range
         ,certific_date
         ,sepa_agr_dt
         ,status_date
         ,fin_range_dt
         ,tmstamp
         ,date_of_birth
         ,expire_date
         ,doc_expire_date
         ,customer_begin_dat
         ,legal_expire_date
         ,employement_start
         ,last_update
         ,certific_cust
         ,sepa_agr_flg
         ,non_resident_for_r
         ,fax_indicator
         ,major_beneficiary
         ,business_ind
         ,mail_ind
         ,entry_status
         ,cust_type
         ,non_registered
         ,cust_status
         ,sex
         ,non_resident
         ,no_afm
         ,prohibit_withdraw
         ,pensioner_ind
         ,institute_inv_ind
         ,self_indicator
         ,title
         ,fk_cust_bankempid
         ,fk_bankemployeeid
         ,fk0bankemployeeid
         ,fk_usrcode
         ,chamber_id
         ,telex
         ,short_name
         ,latin_firstname
         ,birthplace
         ,first_name
         ,incomplete_u_comnt
         ,alert_msg
         ,father_surname
         ,self_name
         ,second_surname
         ,employer
         ,employer_address
         ,attraction_person
         ,e_mail
         ,surname
         ,latin_surname
         ,promocode
         ,mobile_tel
         ,telephone_1
         ,middle_name
         ,father_name
         ,mother_name
         ,spouse_name
         ,chamber_comments
         ,mother_surname
         ,marketing_reminder
         ,internet_address
         ,entry_comments
         ,not_res_bop
         ,city_of_birth
         ,aml_status
         ,turnover_amn
         ,cust_open_date
         ,no_of_businesses
         ,name_standard
         ,bankemployee_name
         ,address
         ,telephone
         ,id_no
         ,id_issue_authority
         ,national_description
         ,tax_registration_no
         ,city
         ,photo_flag
         ,signature_flag
         ,cust_type_ind
         ,bankemployee_id
   FROM   (SELECT cust_id
                 ,c_digit
                 ,num_of_children
                 ,family_members
                 ,fkunit_belongs
                 ,fkunit_is_monitore
                 ,fkcurr_thinks_in
                 ,fin_range
                 ,certific_date
                 ,sepa_agr_dt
                 ,status_date
                 ,fin_range_dt
                 ,tmstamp
                 ,date_of_birth
                 ,expire_date
                 ,doc_expire_date
                 ,customer_begin_dat
                 ,legal_expire_date
                 ,employement_start
                 ,last_update
                 ,certific_cust
                 ,sepa_agr_flg
                 ,non_resident_for_r
                 ,fax_indicator
                 ,major_beneficiary
                 ,business_ind
                 ,mail_ind
                 ,entry_status
                 ,cust_type
                 ,non_registered
                 ,cust_status
                 ,sex
                 ,non_resident
                 ,no_afm
                 ,prohibit_withdraw
                 ,pensioner_ind
                 ,institute_inv_ind
                 ,self_indicator
                 ,title
                 ,fk_cust_bankempid
                 ,fk_bankemployeeid
                 ,fk0bankemployeeid
                 ,fk_usrcode
                 ,chamber_id
                 ,telex
                 ,short_name
                 ,latin_firstname
                 ,birthplace
                 ,first_name
                 ,incomplete_u_comnt
                 ,alert_msg
                 ,father_surname
                 ,self_name
                 ,second_surname
                 ,employer
                 ,employer_address
                 ,attraction_person
                 ,e_mail
                 ,surname
                 ,latin_surname
                 ,promocode
                 ,mobile_tel
                 ,telephone_1
                 ,middle_name
                 ,father_name
                 ,mother_name
                 ,spouse_name
                 ,chamber_comments
                 ,mother_surname
                 ,marketing_reminder
                 ,internet_address
                 ,entry_comments
                 ,not_res_bop
                 ,city_of_birth
                 ,aml_status
                 ,turnover_amn
                 ,cust_open_date
                 ,no_of_businesses
                 ,name_standard
                 ,bankemployee_name
                 ,address
                 ,telephone
                 ,id_no
                 ,id_issue_authority
                 ,national_description
                 ,tax_registration_no
                 ,city
                 ,photo_flag
                 ,signature_flag
                 ,cust_type_ind
                 ,bankemployee_id
           FROM   w_stg_customer
           MINUS
           SELECT cust_id
                 ,c_digit
                 ,num_of_children
                 ,family_members
                 ,fkunit_belongs
                 ,fkunit_is_monitore
                 ,fkcurr_thinks_in
                 ,fin_range
                 ,certific_date
                 ,sepa_agr_dt
                 ,status_date
                 ,fin_range_dt
                 ,tmstamp
                 ,date_of_birth
                 ,expire_date
                 ,doc_expire_date
                 ,customer_begin_dat
                 ,legal_expire_date
                 ,employement_start
                 ,last_update
                 ,certific_cust
                 ,sepa_agr_flg
                 ,non_resident_for_r
                 ,fax_indicator
                 ,major_beneficiary
                 ,business_ind
                 ,mail_ind
                 ,entry_status
                 ,cust_type
                 ,non_registered
                ,cust_status
                 ,sex
                 ,non_resident
                 ,no_afm
                 ,prohibit_withdraw
                 ,pensioner_ind
                 ,institute_inv_ind
                 ,self_indicator
                 ,title
                 ,fk_cust_bankempid
                 ,fk_bankemployeeid
                 ,fk0bankemployeeid
                 ,fk_usrcode
                 ,chamber_id
                 ,telex
                 ,short_name
                 ,latin_firstname
                 ,birthplace
                 ,first_name
                 ,incomplete_u_comnt
                 ,alert_msg
                 ,father_surname
                 ,self_name
                 ,second_surname
                 ,employer
                 ,employer_address
                 ,attraction_person
                 ,e_mail
                 ,surname
                 ,latin_surname
                 ,promocode
                 ,mobile_tel
                 ,telephone_1
                 ,middle_name
                 ,father_name
                 ,mother_name
                 ,spouse_name
                 ,chamber_comments
                 ,mother_surname
                 ,marketing_reminder
                 ,internet_address
                 ,entry_comments
                 ,not_res_bop
                 ,city_of_birth
                 ,aml_status
                 ,turnover_amn
                 ,cust_open_date
                 ,no_of_businesses
                 ,name_standard
                 ,bankemployee_name
                 ,address
                 ,telephone
                 ,id_no
                 ,id_issue_authority
                 ,national_description
                 ,tax_registration_no
                 ,city
                 ,photo_flag
                 ,signature_flag
                 ,cust_type_ind
                 ,bankemployee_id
           FROM   w_dim_customer
           WHERE  row_current_flag = 1);
END;

