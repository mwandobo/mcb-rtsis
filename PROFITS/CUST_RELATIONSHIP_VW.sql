CREATE VIEW CUST_RELATIONSHIP_VW  AS  WITH o          AS (SELECT fk_customercust_id, MIN (serial_no) serial_no              FROM   other_afm              WHERE  main_flag = '1'              GROUP BY fk_customercust_id)         ,oo          AS (SELECT *              FROM   other_afm              WHERE  (fk_customercust_id, serial_no) IN (SELECT fk_customercust_id                                                               ,serial_no                                                         FROM   o))         ,t          AS (SELECT fkcust_has_as_seco, fkcust_has_as_firs, fk_relationshiptyp              FROM   relationship)     SELECT c2.cust_id related_cust_id           ,c2.first_name related_first_name           ,c2.surname related_to_surname           ,TRIM (c2.first_name) || ' ' || TRIM (c2.surname) related_full_name           ,c2.cust_type related_cust_type           ,o2.afm_no related_afm_no           ,relationship_type.type_id           ,rel_description           ,c1.cust_id           ,c1.first_name first_name           ,c1.surname surname           ,TRIM (c1.first_name) || ' ' || TRIM (c1.surname) full_name           ,c1.cust_type           ,o1.afm_no afm_no     FROM   customer c1            LEFT JOIN t ON (fkcust_has_as_seco = c1.cust_id --                 AND fk_relationshiptyp = 'REPRESENTS'
                                                         )            LEFT JOIN relationship_type ON (type_id = fk_relationshiptyp)            LEFT JOIN customer c2 ON (fkcust_has_as_firs = c2.cust_id)            LEFT JOIN oo o1 ON (o1.fk_customercust_id = c1.cust_id)            LEFT JOIN oo o2 ON (o2.fk_customercust_id = c2.cust_id)  WITH NO ROW MOVEMENT;

comment on table CUST_RELATIONSHIP_VW is 'This is bridge hierarchy between the two related Customers.

Depedencies (Uses):
"Type"."Name"."Parent"
TABLE.CUSTOMER.
TABLE.OTHER_AFM.
TABLE.RELATIONSHIP.
TABLE.RELATIONSHIP_TYPE.';

