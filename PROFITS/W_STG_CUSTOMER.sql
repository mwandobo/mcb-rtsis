create table W_STG_CUSTOMER
(
    CUST_ID              INTEGER,
    C_DIGIT              SMALLINT,
    CLEANESS             SMALLINT,
    CHILDREN_ABOVE18     SMALLINT,
    NUM_OF_CHILDREN      SMALLINT,
    FAMILY_MEMBERS       SMALLINT,
    FK_BRANCH_PORTFPOR   INTEGER,
    FK_BRANCH_PORTFBRA   INTEGER,
    FKUNIT_BELONGS       INTEGER,
    FKUNIT_IS_MONITORE   INTEGER,
    FKCURR_HAS_AS_LIMI   INTEGER,
    FKCURR_THINKS_IN     INTEGER,
    FKUNIT_IS_SERVICED   INTEGER,
    FK_DISTR_CHANNEID    INTEGER,
    FK_BISS_CODE         INTEGER,
    SELF_NUM             INTEGER,
    OLD_CUST_ID          DECIMAL(10),
    LIMIT                DECIMAL(15, 2),
    CORRESPONDENT_LIMI   DECIMAL(15, 2),
    FIN_RANGE            DECIMAL(15, 2),
    CERTIFIC_DATE        DATE,
    SEPA_AGR_DT          DATE,
    STATUS_DATE          DATE,
    FIN_RANGE_DT         DATE,
    TMSTAMP              DATE,
    DATE_OF_BIRTH        DATE,
    EXPIRE_DATE          DATE,
    DOC_EXPIRE_DATE      DATE,
    CUSTOMER_BEGIN_DAT   DATE,
    LEGAL_EXPIRE_DATE    DATE,
    EMPLOYEMENT_START    DATE,
    LAST_UPDATE          DATE,
    CERTIFIC_CUST        CHAR(1),
    SEPA_AGR_FLG         CHAR(1),
    NON_RESIDENT_FOR_R   CHAR(1),
    FAX_INDICATOR        CHAR(1),
    MAJOR_BENEFICIARY    CHAR(1),
    BUSINESS_IND         CHAR(1),
    MAIL_IND             CHAR(1),
    SUN_NONWORK          CHAR(1),
    SAT_NONWORK          CHAR(1),
    ENTRY_STATUS         CHAR(1),
    CUST_TYPE            CHAR(1),
    VIP_IND              CHAR(1),
    BLACKLISTED_IND      CHAR(1),
    NON_REGISTERED       CHAR(1),
    CUST_STATUS          CHAR(1),
    SEX                  CHAR(1),
    NON_RESIDENT         CHAR(1),
    NO_AFM               CHAR(1),
    TELEX_CONNECTION     CHAR(1),
    SWIFT_CONNECTION_I   CHAR(1),
    NOSTRO_ACCOUNT_IND   CHAR(1),
    VOSTRO_ACCOUNT_IND   CHAR(1),
    PROHIBIT_WITHDRAW    CHAR(1),
    NON_PROFIT           CHAR(1),
    CONSOLID_STATM_FLG   CHAR(1),
    PENSIONER_IND        CHAR(1),
    INSTITUTE_INV_IND    CHAR(1),
    SELF_INDICATOR       CHAR(1),
    SEGM_FLAGS           CHAR(5),
    TITLE                CHAR(6),
    SPM_NUMBER           CHAR(7),
    FK_CUST_BANKEMPID    CHAR(8),
    FK_BANKEMPLOYEEID    CHAR(8),
    FK0BANKEMPLOYEEID    CHAR(8),
    FK_USRCODE           CHAR(8),
    CHAMBER_ID           CHAR(10),
    DAI_NUMBER           CHAR(12),
    TELEX                CHAR(15),
    SHORT_NAME           CHAR(15),
    LATIN_FIRSTNAME      CHAR(20),
    BIRTHPLACE           CHAR(20),
    FIRST_NAME           CHAR(20),
    FK_GLG_ACCOUNTACCO   CHAR(21),
    INCOMPLETE_U_COMNT   CHAR(30),
    ALERT_MSG            CHAR(30),
    FATHER_SURNAME       CHAR(40),
    SELF_NAME            CHAR(40),
    SECOND_SURNAME       CHAR(40),
    EMPLOYER             CHAR(40),
    EMPLOYER_ADDRESS     CHAR(40),
    ATTRACTION_PERSON    CHAR(50),
    E_MAIL               CHAR(64),
    SURNAME              CHAR(70),
    LATIN_SURNAME        CHAR(70),
    CLEANESS_COMMENTS    CHAR(80),
    PROMOCODE            VARCHAR(6),
    SWIFT_ADDRESS        VARCHAR(12),
    MOBILE_TEL           VARCHAR(15),
    TELEPHONE_1          VARCHAR(15),
    MIDDLE_NAME          VARCHAR(15),
    FATHER_NAME          VARCHAR(20),
    MOTHER_NAME          VARCHAR(20),
    SPOUSE_NAME          VARCHAR(20),
    CHAMBER_COMMENTS     VARCHAR(30),
    MOTHER_SURNAME       VARCHAR(40),
    ATTRACTION_DETAILS   VARCHAR(40),
    MARKETING_REMINDER   VARCHAR(80),
    INTERNET_ADDRESS     VARCHAR(100),
    ENTRY_COMMENTS       VARCHAR(254),
    FICLI_DESC           CHAR(42),
    FICLI_CODE           INTEGER,
    NOT_RES_BOP          CHAR(1),
    REF_B_TRX_USR_SN     INTEGER,
    REF_D_TRX_USR_SN     INTEGER,
    REF_TRX_USR          CHAR(8),
    REF_TRX_DATE         DATE,
    REF_TRX_UNIT         INTEGER,
    REF_DEP_ACC          DECIMAL(11),
    REF_CUSTID           INTEGER,
    CITY_OF_BIRTH        CHAR(20),
    REPR_PHONE           CHAR(15),
    REPR_SURNAME         CHAR(70),
    REPR_FIRSTNAME       CHAR(20),
    IBAN_ACCOUNT         CHAR(27),
    BASEL_STATUS         CHAR(25),
    BASEL_DESCRIPTION    CHAR(90),
    AML_STATUS           CHAR(1),
    TURNOVER_AMN         DECIMAL(15, 2),
    LOANS_AMN            DECIMAL(15, 2),
    PROFIT_AMN           DECIMAL(15, 2),
    PROFITABILITY_AMN    DECIMAL(15, 2),
    SALARY_AMN           DECIMAL(15, 2),
    ENABLE_FOR_24C       CHAR(1),
    CUST_OPEN_DATE       DATE,
    NO_OF_BUSINESSES     INTEGER,
    OWNERSHIP_INDICATION CHAR(1),
    CONTRACT_EXPIRY_DATE DATE,
    CONTRACT             CHAR(1),
    MOBILE_TEL2          VARCHAR(15),
    E_MAIL2              VARCHAR(64),
    NAME_STANDARD        VARCHAR(107),
    BANKEMPLOYEE_NAME    VARCHAR(41),
    SPM_NUMERIC          CHAR(7),
    DAI_NUMERIC          CHAR(12),
    ADDRESS              VARCHAR(245),
    TELEPHONE            VARCHAR(15),
    NATIONAL_DESCRIPTION VARCHAR(40),
    ID_NO                CHAR(20),
    ID_ISSUE_AUTHORITY   CHAR(30),
    TAX_REGISTRATION_NO  VARCHAR(20),
    CITY                 VARCHAR(30),
    CUST_TYPE_IND        VARCHAR(12),
    PHOTO_FLAG           VARCHAR(8),
    SIGNATURE_FLAG       VARCHAR(12),
    BANKEMPLOYEE_ID      VARCHAR(8)
);

create unique index W_STG_CUSTOMER_PK
    on W_STG_CUSTOMER (CUST_ID);

CREATE PROCEDURE W_STG_CUSTOMER ( )
  SPECIFIC SQL160620112633146
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_stg_customer;
INSERT INTO w_stg_customer (
               cust_id
              ,c_digit
              ,cleaness
              ,children_above18
              ,num_of_children
              ,family_members
              ,fk_branch_portfpor
              ,fk_branch_portfbra
              ,fkunit_belongs
              ,fkunit_is_monitore
              ,fkcurr_has_as_limi
              ,fkcurr_thinks_in
              ,fkunit_is_serviced
              ,fk_distr_channeid
              ,fk_biss_code
              ,self_num
              ,old_cust_id
              ,LIMIT
              ,correspondent_limi
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
              ,sun_nonwork
              ,sat_nonwork
              ,entry_status
              ,cust_type
              ,vip_ind
              ,blacklisted_ind
              ,non_registered
              ,cust_status
              ,sex
              ,non_resident
              ,no_afm
              ,telex_connection
              ,swift_connection_i
              ,nostro_account_ind
              ,vostro_account_ind
              ,prohibit_withdraw
              ,non_profit
              ,consolid_statm_flg
              ,pensioner_ind
              ,institute_inv_ind
              ,self_indicator
              ,segm_flags
              ,title
              ,spm_number
              ,fk_cust_bankempid
              ,fk_bankemployeeid
              ,fk0bankemployeeid
              ,fk_usrcode
              ,chamber_id
              ,dai_number
              ,telex
              ,short_name
              ,latin_firstname
              ,birthplace
              ,first_name
              ,fk_glg_accountacco
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
              ,cleaness_comments
              ,promocode
              ,swift_address
              ,mobile_tel
              ,telephone_1
              ,middle_name
              ,father_name
              ,mother_name
              ,spouse_name
              ,chamber_comments
              ,mother_surname
              ,attraction_details
              ,marketing_reminder
              ,internet_address
              ,entry_comments
              ,ficli_desc
              ,ficli_code
              ,not_res_bop
              ,ref_b_trx_usr_sn
              ,ref_d_trx_usr_sn
              ,ref_trx_usr
              ,ref_trx_date
              ,ref_trx_unit
              ,ref_dep_acc
              ,ref_custid
              ,city_of_birth
              ,repr_phone
              ,repr_surname
              ,repr_firstname
              ,iban_account
              ,basel_status
              ,basel_description
              ,aml_status
              ,turnover_amn
              ,loans_amn
              ,profit_amn
              ,profitability_amn
              ,salary_amn
              ,enable_for_24c
              ,cust_open_date
              ,no_of_businesses
              ,ownership_indication
              ,contract_expiry_date
              ,contract
              ,mobile_tel2
              ,e_mail2
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
   WITH sc
        AS (SELECT   cust_id
                    ,MAX (
                        NVL2 (
                           DECODE (image_type, '1', image_id)
                          ,'Photo'
                          ,'No Photo'))
                        photo_flag
                    ,MAX (
                        NVL2 (
                           DECODE (image_type, '2', image_id)
                          ,'Signature'
                          ,'No Signature'))
                        signature_flag
            FROM     w_stg_image_customer
            GROUP BY cust_id)
       ,afm
        AS (SELECT   fk_customercust_id cust_id
                    ,DECODE (
                        MAX (main_flag)
                       ,'1', MAX (DECODE (main_flag, '1', afm_no))
                       ,MAX (afm_no))
                        tax_registration_no
            FROM     other_afm afm
            WHERE    (   (SELECT scheduled_date FROM bank_parameters) BETWEEN issue_date
                                                                          AND expiry_date
                      OR expiry_date = DATE '0001-01-01')
            GROUP BY fk_customercust_id)
       ,ccategory
        AS (SELECT customer_category.*, fk_generic_headpar, gd.description
            FROM   customer_category, generic_detail gd
            WHERE      fk_generic_detafk = gd.fk_generic_headpar
                   AND fk_generic_detaser = gd.serial_num
                   AND gd.fk_generic_headpar = gd.fk_generic_headpar)
       ,natio
        AS (SELECT description, fk_customercust_id
            FROM   ccategory
            WHERE      fk_categorycategor = 'NATIONAL'
                   AND fk_generic_headpar = 'NATIO')
   SELECT c.cust_id
         ,c.c_digit
         ,c.cleaness
         ,c.children_above18
         ,c.num_of_children
         ,c.family_members
         ,c.fk_branch_portfpor
         ,c.fk_branch_portfbra
         ,c.fkunit_belongs
         ,c.fkunit_is_monitore
         ,c.fkcurr_has_as_limi
         ,c.fkcurr_thinks_in
         ,c.fkunit_is_serviced
         ,c.fk_distr_channeid
         ,c.fk_biss_code
         ,c.self_num
         ,c.old_cust_id
         ,c.LIMIT
         ,c.correspondent_limi
         ,c.fin_range
         ,c.certific_date
         ,c.sepa_agr_dt
         ,c.status_date
         ,c.fin_range_dt
         ,c.tmstamp
         ,c.date_of_birth
         ,c.expire_date
         ,c.doc_expire_date
         ,c.customer_begin_dat
         ,c.legal_expire_date
         ,c.employement_start
         ,c.last_update
         ,c.certific_cust
         ,c.sepa_agr_flg
         ,c.non_resident_for_r
         ,c.fax_indicator
         ,c.major_beneficiary
         ,c.business_ind
         ,c.mail_ind
         ,c.sun_nonwork
         ,c.sat_nonwork
         ,c.entry_status
         ,c.cust_type
         ,c.vip_ind
         ,c.blacklisted_ind
         ,c.non_registered
         ,c.cust_status
         ,c.sex
         ,c.non_resident
         ,c.no_afm
         ,c.telex_connection
         ,c.swift_connection_i
         ,c.nostro_account_ind
         ,c.vostro_account_ind
         ,c.prohibit_withdraw
         ,c.non_profit
         ,c.consolid_statm_flg
         ,c.pensioner_ind
         ,c.institute_inv_ind
         ,c.self_indicator
         ,c.segm_flags
         ,c.title
         ,c.spm_number
         ,c.fk_cust_bankempid
         ,c.fk_bankemployeeid
         ,c.fk0bankemployeeid
         ,c.fk_usrcode
         ,c.chamber_id
         ,c.dai_number
         ,c.telex
         ,c.short_name
         ,c.latin_firstname
         ,c.birthplace
         ,c.first_name
         ,c.fk_glg_accountacco
         ,c.incomplete_u_comnt
         ,c.alert_msg
         ,c.father_surname
         ,c.self_name
         ,c.second_surname
         ,c.employer
         ,c.employer_address
         ,c.attraction_person
         ,c.e_mail
         ,c.surname
         ,c.latin_surname
         ,c.cleaness_comments
         ,c.promocode
         ,c.swift_address
         ,c.mobile_tel
         ,c.telephone_1
         ,c.middle_name
         ,c.father_name
         ,c.mother_name
         ,c.spouse_name
         ,c.chamber_comments
         ,c.mother_surname
         ,c.attraction_details
         ,c.marketing_reminder
         ,c.internet_address
         ,c.entry_comments
         ,c.ficli_desc
         ,c.ficli_code
         ,c.not_res_bop
         ,c.ref_b_trx_usr_sn
         ,c.ref_d_trx_usr_sn
         ,c.ref_trx_usr
         ,c.ref_trx_date
         ,c.ref_trx_unit
         ,c.ref_dep_acc
         ,c.ref_custid
         ,c.city_of_birth
         ,c.repr_phone
         ,c.repr_surname
         ,c.repr_firstname
         ,c.iban_account
         ,c.basel_status
         ,c.basel_description
         ,c.aml_status
         ,c.turnover_amn
         ,c.loans_amn
         ,c.profit_amn
         ,c.profitability_amn
         ,c.salary_amn
         ,c.enable_for_24c
         ,c.cust_open_date
         ,c.no_of_businesses
         ,c.ownership_indication
         ,c.contract_expiry_date
         ,c.contract
         ,c.mobile_tel2
         ,c.e_mail2
         ,TRIM (
             CASE
                WHEN c.cust_type = 1
                THEN
                      TRIM (NVL (c.first_name, ' '))
                   || ' '
                   || TRIM (NVL (c.middle_name, ' '))
                   || ' '
                   || TRIM (NVL (c.surname, ' '))
                ELSE
                   TRIM (c.surname)
             END)
             name_standard
         ,TRIM (bankemployee.first_name) || ' ' || bankemployee.last_name
             bankemployee_name
         ,caddr.address
         ,caddr.telephone
         ,id_no
         ,issue_authority id_issue_authority
         ,natio.description national_description
         ,afm.tax_registration_no
         ,caddr.city
         ,NVL (sc.photo_flag, 'No Photo') photo_flag
         ,NVL (sc.signature_flag, 'No Signature') signature_flag
         ,DECODE (
             cust_type
            ,'1', 'Natural'
            ,'2', 'Corporate'
            ,'3', 'Correspodent'
            ,'n/a')
             cust_type_ind
         ,bankemployee.id bankemployee_id
   FROM   customer c
          LEFT JOIN bankemployee ON bankemployee.id = c.fk_cust_bankempid
          LEFT JOIN
          (SELECT   cust_address.fk_customercust_id cust_id
                   ,MAX (
                          TRIM (address_1)
                       || ' '
                       || TRIM (address_2)
                       || ' '
                       || TRIM (address_3)
                       || ' '
                       || TRIM (address_4)
                       || ' '
                       || TRIM (address_5)
                       || ' '
                       || TRIM (address_6))
                       address
                   ,MAX (telephone) telephone
                   ,MAX (city) city
           FROM     cust_address
           WHERE    communication_addr = '1' AND entry_status = '1'
           GROUP BY cust_address.fk_customercust_id) caddr
             ON caddr.cust_id = c.cust_id
          LEFT JOIN other_id id
             ON (    NVL2 (id.serial_no, id.main_flag, '1') = '1'
                 AND id.fk_customercust_id = c.cust_id)
          LEFT JOIN natio ON (natio.fk_customercust_id = c.cust_id)
          LEFT JOIN afm ON afm.cust_id = c.cust_id
          LEFT JOIN sc ON sc.cust_id = c.cust_id AND c.cust_type = '1';
END;

