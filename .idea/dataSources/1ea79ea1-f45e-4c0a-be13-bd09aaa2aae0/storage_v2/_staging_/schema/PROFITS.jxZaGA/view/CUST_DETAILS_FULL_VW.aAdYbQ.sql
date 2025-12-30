CREATE VIEW CUST_DETAILS_FULL_VW
            (CUST_ID, C_DIGIT, CLEANESS, CHILDREN_ABOVE18, NUM_OF_CHILDREN, FAMILY_MEMBERS, FK_BRANCH_PORTFPOR,
             FK_BRANCH_PORTFBRA, FKUNIT_BELONGS, FKUNIT_IS_MONITORE, FKCURR_HAS_AS_LIMI, FKCURR_THINKS_IN,
             FKUNIT_IS_SERVICED, FK_DISTR_CHANNEID, FK_BISS_CODE, SELF_NUM, OLD_CUST_ID, LIMIT, CORRESPONDENT_LIMI,
             FIN_RANGE, CERTIFIC_DATE, SEPA_AGR_DT, STATUS_DATE, FIN_RANGE_DT, TMSTAMP, DATE_OF_BIRTH, EXPIRE_DATE,
             DOC_EXPIRE_DATE, CUSTOMER_BEGIN_DAT, LEGAL_EXPIRE_DATE, EMPLOYEMENT_START, LAST_UPDATE, CERTIFIC_CUST,
             SEPA_AGR_FLG, NON_RESIDENT_FOR_R, FAX_INDICATOR, MAJOR_BENEFICIARY, BUSINESS_IND, MAIL_IND, SUN_NONWORK,
             SAT_NONWORK, ENTRY_STATUS, CUST_TYPE, VIP_IND, BLACKLISTED_IND, NON_REGISTERED, CUST_STATUS, SEX,
             NON_RESIDENT, NO_AFM, TELEX_CONNECTION, SWIFT_CONNECTION_I, NOSTRO_ACCOUNT_IND, VOSTRO_ACCOUNT_IND,
             PROHIBIT_WITHDRAW, NON_PROFIT, CONSOLID_STATM_FLG, PENSIONER_IND, INSTITUTE_INV_IND, SELF_INDICATOR,
             SEGM_FLAGS, TITLE, SPM_NUMBER, FK_CUST_BANKEMPID, FK_BANKEMPLOYEEID, FK0BANKEMPLOYEEID, FK_USRCODE,
             CHAMBER_ID, DAI_NUMBER, TELEX, SHORT_NAME, LATIN_FIRSTNAME, BIRTHPLACE, FIRST_NAME, FK_GLG_ACCOUNTACCO,
             INCOMPLETE_U_COMNT, ALERT_MSG, FATHER_SURNAME, SELF_NAME, SECOND_SURNAME, EMPLOYER, EMPLOYER_ADDRESS,
             ATTRACTION_PERSON, E_MAIL, SURNAME, LATIN_SURNAME, CLEANESS_COMMENTS, PROMOCODE, SWIFT_ADDRESS, MOBILE_TEL,
             TELEPHONE_1, MIDDLE_NAME, FATHER_NAME, MOTHER_NAME, SPOUSE_NAME, CHAMBER_COMMENTS, MOTHER_SURNAME,
             ATTRACTION_DETAILS, MARKETING_REMINDER, INTERNET_ADDRESS, ENTRY_COMMENTS, FICLI_DESC, FICLI_CODE,
             NOT_RES_BOP, REF_B_TRX_USR_SN, REF_D_TRX_USR_SN, REF_TRX_USR, REF_TRX_DATE, REF_TRX_UNIT, REF_DEP_ACC,
             REF_CUSTID, CITY_OF_BIRTH, REPR_PHONE, REPR_SURNAME, REPR_FIRSTNAME, IBAN_ACCOUNT, BASEL_STATUS,
             BASEL_DESCRIPTION, AML_STATUS, TURNOVER_AMN, LOANS_AMN, PROFIT_AMN, PROFITABILITY_AMN, SALARY_AMN,
             ENABLE_FOR_24C, CUST_OPEN_DATE, NO_OF_BUSINESSES, OWNERSHIP_INDICATION, CONTRACT_EXPIRY_DATE, CONTRACT,
             MOBILE_TEL2, E_MAIL2, AFM_NO, ID_NO, FKGD_HAS_TYPE, OTHER_ID_DESC, AFM_SERIAL_NO, ID_ISSUE_DATE,
             ID_EXPIRY_DATE, ID_SERIAL_NO, ID_COUNTRY_SERIAL_NUM, ID_COUNTRY_FK_GENERIC_HEADPAR, ID_COUNTRY_DESCRIPTION,
             FKGD_HAS_COUNTRY, FKGH_HAS_COUNTRY, C_COUNTRY, CUST_ADVANCES_CLASSIF_DATE, CUST_ADVANCES_DESCRIPTION,
             NATIONAL_DESCRIPTION, CITIZEN_DESCRIPTION, COMLANG, ECONOMY, ACTIVITY, TAX_OFFICE_NAME, FAX_NO, C_CITY,
             C_ZIP_CODE, C_TELEPHONE, C_ADDRESS_1, C_ADDRESS_2, C_REGION, W_FAX_NO, W_CITY, W_ZIP_CODE, W_TELEPHONE,
             W_ADDRESS_1, W_ADDRESS_2, W_REGION, W_COUNTRY, CLASSIF_DATE, PROFLEVL, PROFES, CURRENCY_SHORT, B_COUNTRY,
             HOLDR_FK_GENERIC_DETASER, SHAREHOL, HOLDR_DESCRIPTION, AMLRICAT, BCOUNTRY, CMPTYPE, COMTYPE, CRCAMLC,
             EDULEVEL, FAMILY, FINMARKE, FINRANGE, FINSEGME, INDCODE, LAWSHAPE, PROFCAT, REGION, RSKCISCA, TRADESTS,
             NAME_STANDARD)
AS
WITH w_address AS (SELECT *
                   FROM cust_address c
                   WHERE (c.fk_customercust_id, c.serial_num) IN (SELECT fk_customercust_id, MIN(serial_num)
                                                                  FROM cust_address
                                                                  WHERE address_type = '4'
                                                                    AND entry_status = '1'
                                                                  GROUP BY fk_customercust_id)),
     cai AS (SELECT fk_customercust_id, classif_date, gd.parameter_type, gd.serial_num, gd.description
             FROM generic_detail gd,
                  cust_advances_info,
                  generic_header
             WHERE fk0gen_det_gen_he = gd.fk_generic_headpar
               AND fk0gen_det_sernum = gd.serial_num
               AND generic_header.parameter_type = gd.fk_generic_headpar
               AND generic_header.parameter_type IN ('CRRAT')),
     ccategory AS (SELECT customer_category.*, fk_generic_headpar, gd.description
                   FROM customer_category,
                        generic_detail gd
                   WHERE fk_generic_detafk = gd.fk_generic_headpar
                     AND fk_generic_detaser = gd.serial_num
                     AND gd.fk_generic_headpar = gd.fk_generic_headpar),
     natio AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'NATIONAL' AND fk_generic_headpar = 'NATIO'),
     citiz AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'CITIZEN' AND fk_generic_headpar = 'CITIZ'),
     comla AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'COMLANG' AND fk_generic_headpar = 'COMLA'),
     finsc AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'ECONOMY' AND fk_generic_headpar = 'FINSC'),
     ccode AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'ACTIVITY' AND fk_generic_headpar = 'CCODE'),
     prfst AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'PROFLEVL' AND fk_generic_headpar = 'PRFST'),
     proff AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'PROFES' AND fk_generic_headpar = 'PROFF'),
     holdr AS (SELECT description, fk_customercust_id, fk_generic_detaser /*TODO-> for compatibility issues*/
               FROM ccategory
               WHERE fk_categorycategor = 'SHAREHOL'
                 AND fk_generic_headpar = 'HOLDR'),
     amlri AS (SELECT description, fk_customercust_id
               FROM ccategory
               WHERE fk_categorycategor = 'AMLRICAT' AND fk_generic_headpar = 'AMLRI'),
     bcountry AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'BCOUNTRY' AND fk_generic_headpar = 'CNTRY'),
     cmptype AS (SELECT description, fk_customercust_id
                 FROM ccategory
                 WHERE fk_categorycategor = 'CMPTYPE' AND fk_generic_headpar = 'COMTP'),
     comtype AS (SELECT description, fk_customercust_id
                 FROM ccategory
                 WHERE fk_categorycategor = 'COMTYPE' AND fk_generic_headpar = 'CMPTP'),
     crcamlc AS (SELECT description, fk_customercust_id
                 FROM ccategory
                 WHERE fk_categorycategor = 'CRCAMLC' AND fk_generic_headpar = 'AMLRI'),
     edulevel AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'EDULEVEL' AND fk_generic_headpar = 'EDULV'),
     family AS (SELECT description, fk_customercust_id
                FROM ccategory
                WHERE fk_categorycategor = 'FAMILY' AND fk_generic_headpar = 'FALST'),
     finmarke AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'FINMARKE' AND fk_generic_headpar = 'FMARK'),
     finrange AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'FINRANGE' AND fk_generic_headpar = 'FRANG'),
     finsegme AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'FINSEGME' AND fk_generic_headpar = 'COMCL'),
     indcode AS (SELECT description, fk_customercust_id
                 FROM ccategory
                 WHERE fk_categorycategor = 'INDCODE' AND fk_generic_headpar = 'INDCD'),
     lawshape AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'LAWSHAPE' AND fk_generic_headpar = 'LEGAL'),
     profcat AS (SELECT description, fk_customercust_id
                 FROM ccategory
                 WHERE fk_categorycategor = 'PROFCAT' AND fk_generic_headpar = 'EMPTP'),
     region AS (SELECT description, fk_customercust_id
                FROM ccategory
                WHERE fk_categorycategor = 'REGION' AND fk_generic_headpar = 'REGIO'),
     rskcisca AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'RSKCISCA' AND fk_generic_headpar = 'CURSK'),
     tradests AS (SELECT description, fk_customercust_id
                  FROM ccategory
                  WHERE fk_categorycategor = 'TRADESTS' AND fk_generic_headpar = 'TRDST')
SELECT c.cust_id,
       c.c_digit,
       c.cleaness,
       c.children_above18,
       c.num_of_children,
       c.family_members,
       c.fk_branch_portfpor,
       c.fk_branch_portfbra,
       c.fkunit_belongs,
       c.fkunit_is_monitore,
       c.fkcurr_has_as_limi,
       c.fkcurr_thinks_in,
       c.fkunit_is_serviced,
       c.fk_distr_channeid,
       c.fk_biss_code,
       c.self_num,
       c.old_cust_id,
       c.LIMIT,
       c.correspondent_limi,
       c.fin_range,
       c.certific_date,
       c.sepa_agr_dt,
       c.status_date,
       c.fin_range_dt,
       c.tmstamp,
       c.date_of_birth,
       c.expire_date,
       c.doc_expire_date,
       c.customer_begin_dat,
       c.legal_expire_date,
       c.employement_start,
       c.last_update,
       c.certific_cust,
       c.sepa_agr_flg,
       c.non_resident_for_r,
       c.fax_indicator,
       c.major_beneficiary,
       c.business_ind,
       c.mail_ind,
       c.sun_nonwork,
       c.sat_nonwork,
       c.entry_status,
       c.cust_type,
       c.vip_ind,
       c.blacklisted_ind,
       c.non_registered,
       c.cust_status,
       c.sex,
       c.non_resident,
       c.no_afm,
       c.telex_connection,
       c.swift_connection_i,
       c.nostro_account_ind,
       c.vostro_account_ind,
       c.prohibit_withdraw,
       c.non_profit,
       c.consolid_statm_flg,
       c.pensioner_ind,
       c.institute_inv_ind,
       c.self_indicator,
       c.segm_flags,
       c.title,
       c.spm_number,
       c.fk_cust_bankempid,
       c.fk_bankemployeeid,
       c.fk0bankemployeeid,
       c.fk_usrcode,
       c.chamber_id,
       c.dai_number,
       c.telex,
       c.short_name,
       c.latin_firstname,
       c.birthplace,
       c.first_name,
       c.fk_glg_accountacco,
       c.incomplete_u_comnt,
       c.alert_msg,
       c.father_surname,
       c.self_name,
       c.second_surname,
       c.employer,
       c.employer_address,
       c.attraction_person,
       c.e_mail,
       c.surname,
       c.latin_surname,
       c.cleaness_comments,
       c.promocode,
       c.swift_address,
       c.mobile_tel,
       c.telephone_1,
       c.middle_name,
       c.father_name,
       c.mother_name,
       c.spouse_name,
       c.chamber_comments,
       c.mother_surname,
       c.attraction_details,
       c.marketing_reminder,
       c.internet_address,
       c.entry_comments,
       c.ficli_desc,
       c.ficli_code,
       c.not_res_bop,
       c.ref_b_trx_usr_sn,
       c.ref_d_trx_usr_sn,
       c.ref_trx_usr,
       c.ref_trx_date,
       c.ref_trx_unit,
       c.ref_dep_acc,
       c.ref_custid,
       c.city_of_birth,
       c.repr_phone,
       c.repr_surname,
       c.repr_firstname,
       c.iban_account,
       c.basel_status,
       c.basel_description,
       c.aml_status,
       c.turnover_amn,
       c.loans_amn,
       c.profit_amn,
       c.profitability_amn,
       c.salary_amn,
       c.enable_for_24c,
       c.cust_open_date,
       c.no_of_businesses,
       c.ownership_indication,
       c.contract_expiry_date,
       c.contract,
       c.mobile_tel2,
       c.e_mail2,
       afm.afm_no,
       id.id_no,
       id.fkgd_has_type,
       idt.description                                               other_id_desc,
       afm.serial_no                                                 afm_serial_no,
       id.issue_date                                                 id_issue_date,
       id.expiry_date                                                id_expiry_date,
       id.serial_no                                                  id_serial_no,
       id_country.serial_num                                         id_country_serial_num,
       id_country.fk_generic_headpar                                 id_country_fk_generic_headpar,
       id_country.description                                        id_country_description,
       c_address.fkgd_has_country,
       c_address.fkgh_has_country,
       c_country.description                                         c_country,
       cai.classif_date                                              cust_advances_classif_date, --cai_classif_date,
       --          cai.parameter_type cai_parameter_type,
       --          cai.serial_num cai_serial_num,
       cai.description                                               cust_advances_description,--          natio.fk_categorycategor,
       --          natio.fk_generic_detafk natio_fk_generic_detafk,
       --          natio.fk_generic_detaser natio_fk_generic_detaser,
       natio.description                                             national_description,--          citiz.fk_generic_detafk citiz_fk_generic_detafk,
       --          citiz.fk_generic_detaser citiz_fk_generic_detaser,
       citiz.description                                             citizen_description,--          comla.fk_generic_detafk comla_fk_generic_detafk,
       --          comla.fk_generic_detaser comla_fk_generic_detaser,
       comla.description                                             comlang,--          finsc.fk_generic_detafk finsc_fk_generic_detafk,
       --          finsc.fk_generic_detaser finsc_fk_generic_detaser,
       finsc.description                                             economy,--          ccode.fk_generic_detafk ccode_fk_generic_detafk,
       --          ccode.fk_generic_detaser ccode_fk_generic_detaser,
       ccode.description                                             activity,
       tax_office_name,
       c_address.fax_no,
       c_address.city                                                c_city,
       c_address.zip_code                                            c_zip_code,
       c_address.telephone                                           c_telephone,
       c_address.address_1                                           c_address_1,
       c_address.address_2                                           c_address_2,
       c_address.region                                              c_region,
       w_address.fax_no                                              w_fax_no,
       w_address.city                                                w_city,
       w_address.zip_code                                            w_zip_code,
       w_address.telephone                                           w_telephone,
       w_address.address_1                                           w_address_1,
       w_address.address_2                                           w_address_2,
       w_address.region                                              w_region,
       w_country.description                                         w_country,
       cai.classif_date,
       prfst.description                                             proflevl,
       proff.description                                             profes,
       currency.short_descr                                          currency_short,
       b_country.description                                         b_country,
       holdr.fk_generic_detaser                                      holdr_fk_generic_detaser,
       holdr.description                                             sharehol,
       holdr.description                                             holdr_description,--          amlri.fk_generic_detaser holdr_fk_generic_detaser,
       amlri.description                                             amlricat,
       bcountry.description                                          bcountry,
       cmptype.description                                           cmptype,
       comtype.description                                           comtype,
       crcamlc.description                                           crcamlc,
       edulevel.description                                          edulevel,
       family.description                                            family,
       finmarke.description                                          finmarke,
       finrange.description                                          finrange,
       finsegme.description                                          finsegme,
       indcode.description                                           indcode,
       lawshape.description                                          lawshape,
       profcat.description                                           profcat,
       region.description                                            region,
       rskcisca.description                                          rskcisca,
       tradests.description                                          tradests,
       TRIM(CASE
                WHEN cust_type = 2 THEN TRIM(surname)
                WHEN cust_type = 1 THEN TRIM(NVL(first_name, ' ')) || ' ' || TRIM(NVL(middle_name, ' ')) || ' ' ||
                                        TRIM(NVL(surname, ' ')) END) name_standard
FROM customer c
         LEFT JOIN cust_address c_address
                   ON (c_address.fk_customercust_id = c.cust_id AND c_address.communication_addr = '1' AND
                       c_address.entry_status = '1')
         LEFT JOIN w_address ON (w_address.fk_customercust_id = c.cust_id)
         LEFT JOIN generic_detail c_country ON (c_address.fkgd_has_country = c_country.serial_num AND
                                                c_address.fkgh_has_country = c_country.fk_generic_headpar)
         LEFT JOIN generic_detail w_country ON (w_address.fkgd_has_country = w_country.serial_num AND
                                                w_address.fkgh_has_country = w_country.fk_generic_headpar)
         LEFT JOIN generic_detail b_country ON (c_address.fkgd_has_country = b_country.serial_num AND
                                                c_address.fkgh_has_country = b_country.fk_generic_headpar)
         LEFT JOIN currency ON (currency.id_currency = c.fkcurr_thinks_in)
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN generic_detail idt
                   ON (idt.fk_generic_headpar = id.fkgh_has_type AND idt.serial_num = id.fkgd_has_type)
         LEFT JOIN other_afm afm ON (afm.fk_customercust_id = c.cust_id AND CASE
                                                                                WHEN (afm.serial_no IS NULL) THEN '1'
                                                                                ELSE CASE
                                                                                         WHEN c.no_afm = '1'
                                                                                             THEN CAST(afm.serial_no AS VARCHAR(2))
                                                                                         ELSE afm.main_flag END END =
                                                                            '1')
         LEFT JOIN tax_office ON (tax_office.id = fk_tax_officeid)
         LEFT JOIN cai ON (cai.fk_customercust_id = c.cust_id)
         LEFT JOIN natio ON (natio.fk_customercust_id = c.cust_id)
         LEFT JOIN citiz ON (citiz.fk_customercust_id = c.cust_id)
         LEFT JOIN comla ON (comla.fk_customercust_id = c.cust_id)
         LEFT JOIN finsc ON (finsc.fk_customercust_id = c.cust_id)
         LEFT JOIN ccode ON (ccode.fk_customercust_id = c.cust_id)
         LEFT JOIN prfst ON (prfst.fk_customercust_id = c.cust_id)
         LEFT JOIN proff ON (proff.fk_customercust_id = c.cust_id)
         LEFT JOIN holdr ON (holdr.fk_customercust_id = c.cust_id)
         LEFT JOIN amlri ON (amlri.fk_customercust_id = c.cust_id)
         LEFT JOIN bcountry ON (bcountry.fk_customercust_id = c.cust_id)
         LEFT JOIN cmptype ON (cmptype.fk_customercust_id = c.cust_id)
         LEFT JOIN comtype ON (comtype.fk_customercust_id = c.cust_id)
         LEFT JOIN crcamlc ON (crcamlc.fk_customercust_id = c.cust_id)
         LEFT JOIN edulevel ON (edulevel.fk_customercust_id = c.cust_id)
         LEFT JOIN family ON (family.fk_customercust_id = c.cust_id)
         LEFT JOIN finmarke ON (finmarke.fk_customercust_id = c.cust_id)
         LEFT JOIN finrange ON (finrange.fk_customercust_id = c.cust_id)
         LEFT JOIN finsegme ON (finsegme.fk_customercust_id = c.cust_id)
         LEFT JOIN indcode ON (indcode.fk_customercust_id = c.cust_id)
         LEFT JOIN lawshape ON (lawshape.fk_customercust_id = c.cust_id)
         LEFT JOIN profcat ON (profcat.fk_customercust_id = c.cust_id)
         LEFT JOIN region ON (region.fk_customercust_id = c.cust_id)
         LEFT JOIN rskcisca ON (rskcisca.fk_customercust_id = c.cust_id)
         LEFT JOIN tradests ON (tradests.fk_customercust_id = c.cust_id)
WITH NO ROW MOVEMENT;

