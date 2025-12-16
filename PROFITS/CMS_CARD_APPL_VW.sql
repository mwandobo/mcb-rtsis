create view cms_card_appl_vw
as
select * from (
SELECT
                   CMS_CARD_APPL01.*
           FROM
               CMS_CARD_APPL                      CMS_CARD_APPL01,
               GENERIC_DETAIL                      GENERIC_DETAIL02,
               CMS_PARAMS                      CMS_PARAMS03,
               CUSTOMER                      CUSTOMER04
           WHERE
           (
                CMS_CARD_APPL01.APPLICATION_SN > 0
                      AND ((CMS_CARD_APPL01.ENTRY_STATUS = '2'
                     AND CMS_CARD_APPL01.CARD_SN = 0) OR
                     (CMS_CARD_APPL01.ENTRY_STATUS = '4' AND
                     CMS_CARD_APPL01.CARD_SN > 0)) AND
                     CMS_CARD_APPL01.FK_CRDTYP_GENERIC_HD =
                     GENERIC_DETAIL02.FK_GENERIC_HEADPAR  AND
                     CMS_CARD_APPL01.FK_CRDTYP_GENERIC_SN =
                     GENERIC_DETAIL02.SERIAL_NUM AND
                     CMS_PARAMS03.FK_CRDTYP_GENERIC_HD =
                     GENERIC_DETAIL02.FK_GENERIC_HEADPAR  AND
                     CMS_PARAMS03.FK_CRDTYP_GENERIC_DT =
                     GENERIC_DETAIL02.SERIAL_NUM AND
                     CMS_CARD_APPL01.FK_CUST_ID = CUSTOMER04.CUST_ID
                     AND CMS_PARAMS03.VIRTUAL_CARD_FLG = '0'
           )
           ORDER BY    CMS_CARD_APPL01.APPLICATION_SN   ASC);

