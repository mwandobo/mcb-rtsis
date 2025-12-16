CREATE PROCEDURE PROFITS.LOANS_CRD_BOT_UPDATE ( )
  SPECIFIC SQL160629193832902
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  ------------------------------------------------------------------------------
	---------------------------- customer data START ------------------------------
--	------------------------------------------------------------------------------
  FOR CRD_TYPE AS
  	(select (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) AS REPORTING_DATE,
          CUST_ID, CUST_TYPE , MONTHLY_EXP_AMN, MONTHLY_INC_AMN, SPOUSE_FULLNAME ,
		      DATE_OF_BIRTH, DRIVING_LICENCE_ID, NATIONAL_ID, PERMISSION_ID,
		      VOTER_REG_ID, WARD_ID , ZANZIBAR_ID, PASSPORT_ID
      from LNS_CRD_CUST_DATA ll
	  where exists (select *
					 from LNS_CRD_CUST_SUB_DATA l1
				    where ll.CUST_ID = l1.cust_id ))
    DO
      IF CRD_TYPE.CUST_TYPE = 2 THEN
				-- BOT_10_COMPANY
				Insert into BOT_10_COMPANY (CUSTOMERCODE, REPORTING_DATE) values (trim(CRD_TYPE.CUST_ID), CRD_TYPE.REPORTING_DATE);
				-- BOT_13_COMPANYDATA
				Insert into BOT_13_COMPANYDATA (FK_COMPANY, ESTABLISHMENTDATE, LEGALFORM,
												NUMBEROFEMPLOYEES, REGISTRATIONCOUNTRY, REGISTRATIONNUMBER,
												TAXIDENTIFICATIONNUMBER, TRADENAME, REPORTING_DATE   )
					select 	B10.COMPANY_ID, C.DATE_OF_BIRTH,
							trim(nvl((select (case when GD.SERIAL_NUM = 0 then 9 else GD.SERIAL_NUM end)
								   from customer_category cc, generic_Detail gd
								  where CC.FK_CATEGORYCATEGOR='LAWSHAPE'
									and CC.FK_GENERIC_DETAFK=GD.FK_GENERIC_HEADPAR
									and CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
									AND CC.FK_CUSTOMERCUST_ID = C.CUST_ID
									and c.legal_form = gd.description  ), 9)), --C.LEGAL_FORM,
							trim(C.NO_OF_EMPLOYEES),
							218, -- C.REGISTRATION_COUNTRY n/a,
							222, -- C.REGISTRATION_NUMBER n/a,
							(case when C.TAX_ID = '' then null else C.TAX_ID end), trim(C.SURNAME), CRD_TYPE.REPORTING_DATE
					FROM LNS_CRD_CUST_DATA C , BOT_10_COMPANY B10
					where C.CUST_ID = CRD_TYPE.CUST_ID
					AND B10.CUSTOMERCODE = C.CUST_ID;
 
        -- BOT_12_ADDRESSESCOMPANY
				Insert into BOT_12_ADDRESSESCOMPANY (FK_COMPANY, REPORTING_DATE)
					select B10.COMPANY_ID, CRD_TYPE.REPORTING_DATE
					FROM LNS_CRD_CUST_DATA C , BOT_10_COMPANY B10
					where C.CUST_ID = CRD_TYPE.CUST_ID
						AND B10.CUSTOMERCODE = C.CUST_ID;
--
        -- BOT_15_POSTAL
				Insert into BOT_15_POSTAL (FK_ADDRESSESCOMPANY,COUNTRY,DISTRICT,HOUSENUMBER,
											              REGION,STREETWARD, POBOXNUMBER, CITYTOWNVILLAGE, REPORTING_DATE )
					select B12.ADDRESSESCOMPANY_ID,
							trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
							trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
								from GENERIC_DETAIL GD1
								where GD1.PARAMETER_TYPE = 'ADDDI'
								AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
							trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end )),
              1, --????? REGION NO GENERIC_DETAIL EXISTS ,
							trim(D.STREET_WARD), trim(D.ZIP_CODE),
              trim(D.CITY) ,
					    CRD_TYPE.REPORTING_DATE
          from LNS_CRD_CUST_ADDRESS d ,
						BOT_12_ADDRESSESCOMPANY B12,
						BOT_10_COMPANY B10
					where d.CUST_ID = CRD_TYPE.CUST_ID
						AND B10.CUSTOMERCODE = D.CUST_ID
						AND B12.FK_COMPANY = B10.COMPANY_ID
					  AND d.ADDRESS_TYPE = 'Postal';
 
				-- BOT_28_BUSINESS
				Insert into BOT_28_BUSINESS (FK_ADDRESSESCOMPANY,COUNTRY,DISTRICT,HOUSENUMBER,
											                REGION,STREETWARD, POBOXNUMBER, CITYTOWNVILLAGE, REPORTING_DATE )
					select B12.ADDRESSESCOMPANY_ID,
							trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
							trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
								from GENERIC_DETAIL GD1
								where GD1.PARAMETER_TYPE = 'ADDDI'
								AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
							trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end ))
              , 1, --????? REGION NO GENERIC_DETAIL EXISTS ,
							trim(D.STREET_WARD), trim(D.ZIP_CODE),
              trim(D.CITY),
					    CRD_TYPE.REPORTING_DATE
          from LNS_CRD_CUST_ADDRESS d ,
						BOT_12_ADDRESSESCOMPANY B12,
						BOT_10_COMPANY B10
					where d.CUST_ID = CRD_TYPE.CUST_ID
						AND B10.CUSTOMERCODE = D.CUST_ID
						AND B12.FK_COMPANY = B10.COMPANY_ID
					  AND d.ADDRESS_TYPE = 'Business';
				-- BOT_16_REGISTRATION
				Insert into BOT_16_REGISTRATION (FK_ADDRESSESCOMPANY,COUNTRY,DISTRICT,
												HOUSENUMBER,REGION,STREETWARD,POBOXNUMBER, CITYTOWNVILLAGE,REPORTING_DATE)
					select B12.ADDRESSESCOMPANY_ID,
							trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
							trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
								from GENERIC_DETAIL GD1
								where GD1.PARAMETER_TYPE = 'ADDDI'
								AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
						trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end )),
            1, --????? REGION NO GENERIC_DETAIL EXISTS ,
						trim(D.STREET_WARD), trim(D.ZIP_CODE), trim(D.CITY) , CRD_TYPE.REPORTING_DATE
					from LNS_CRD_CUST_ADDRESS d,
						BOT_12_ADDRESSESCOMPANY B12,
						BOT_10_COMPANY B10
					where d.CUST_ID = CRD_TYPE.CUST_ID
						AND B10.CUSTOMERCODE = D.CUST_ID
						AND B12.FK_COMPANY = B10.COMPANY_ID
						AND d.ADDRESS_TYPE='Registration';
				-- BOT_14_CONTACTSCOMPANY
				Insert into BOT_14_CONTACTSCOMPANY (FK_COMPANY,CELLULARPHONE,EMAIL,FAX,
                                           FIXEDLINE,WEBPAGE,REPORTING_DATE)
					select B10.COMPANY_ID,trim((case when C.CELLULAR_PHONE = '' then  null else C.CELLULAR_PHONE end )),
          trim((case when C.EMAIL = '' then  null else C.EMAIL end )),trim((case when C.FAX = '' then  null else C.FAX end )),trim((case when C.FIXED_LINE = '' then  null else C.FIXED_LINE end )),
          trim((case when C.WEBPAGE = '' then  null else C.WEBPAGE end )),
          CRD_TYPE.REPORTING_DATE
					FROM LNS_CRD_CUST_DATA C ,BOT_10_COMPANY B10
					where C.CUST_ID = CRD_TYPE.CUST_ID
						AND B10.CUSTOMERCODE = C.CUST_ID;
 
     ELSE
				-- BOT_11_INDIVIDUAL
				Insert into BOT_11_INDIVIDUAL (CUSTOMERCODE, REPORTING_DATE)
        values (trim(CRD_TYPE.CUST_ID), CRD_TYPE.REPORTING_DATE );
				-- BOT_63_PERSONALDATA
				Insert into BOT_63_PERSONALDATA (FK_INDIVIDUAL, BIRTHSURNAME, CITIZENSHIP, EDUCATION,
												EMPLOYERNAME, FIRSTNAME, GENDER, INDIVIDUALCLASSIFICATION,
												LASTNAME, MARITALSTATUS, MIDDLENAMES, NATIONALITY,
												NUMBEROFCHILDREN, NUMBEROFSPOUSES, PROFESSION, REPORTING_DATE )
					select BOT11.INDIVIDUAL_ID ,trim(C.BIRTH_SURNAME),
							trim(NVL((SELECT (case when GD.LATIN_DESC = 0 then 218 else gd.serial_num end)
								FROM CUSTOMER_CATEGORY CC,
									GENERIC_DETAIL GD
								WHERE CC.FK_CATEGORYCATEGOR='CITIZEN'
									AND CC.FK_GENERIC_DETAFK='CICRD'
									AND CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
									AND C.CUST_ID = CC.FK_CUSTOMERCUST_ID
									AND GD.LATIN_DESC = C.citizenship ),218)),--C.CITIZENSHIP,
							trim(NVL((SELECT GD.SERIAL_NUM
								FROM CUSTOMER_CATEGORY CC,
									GENERIC_DETAIL GD
								WHERE CC.FK_CATEGORYCATEGOR='EDULEVEL'
									AND CC.FK_GENERIC_DETAFK=GD.FK_GENERIC_HEADPAR
									AND CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
									AND C.CUST_ID = CC.FK_CUSTOMERCUST_ID
									AND GD.LATIN_DESC = C.EDUCATION ),1)),--C.EDUCATION,
							trim(C.EMPLOYER), trim(NVL(C.FIRST_NAME, ' ')),
							trim((CASE WHEN C.GENDER LIKE 'F%' THEN 2 ELSE (CASE WHEN C.GENDER LIKE 'M%' THEN 1 ELSE 1 END)  END )),
							trim((CASE WHEN C.IND_CLASS LIKE 'Indivi%' THEN 1 ELSE (CASE WHEN C.IND_CLASS LIKE 'Sole%' THEN 2 ELSE 1 END)  END )),
							trim(NVL(C.SURNAME, ' ')),
							(SELECT GD.SERIAL_NUM
								FROM GENERIC_DETAIL GD
								WHERE GD.FK_GENERIC_HEADPAR='FALST'
									AND TRIM(GD.DESCRIPTION)=TRIM(C.MARITAL_STS)), --C.MARITAL_STS,
							(case when trim(C.MIDDLE_NAME)='' then null else trim(C.MIDDLE_NAME) end),
							trim(NVL((SELECT TRIM(GD.LATIN_DESC)
								FROM CUSTOMER_CATEGORY CC,
									GENERIC_DETAIL GD
								WHERE CC.FK_CATEGORYCATEGOR='NATIONAL'
									AND CC.FK_GENERIC_DETAFK='NACRD'
									AND CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
									AND C.CUST_ID = CC.FK_CUSTOMERCUST_ID
									AND GD.LATIN_DESC = C.nationality ),218)), -- C.NATIONALITY,
							trim(C.NUM_OF_CHILDREN),trim(C.NUM_OF_SPOUSES),
							trim(NVL((SELECT GD.SERIAL_NUM
								FROM CUSTOMER_CATEGORY CC,
									GENERIC_DETAIL GD
								WHERE CC.FK_CATEGORYCATEGOR='PROFES'
									AND CC.FK_GENERIC_DETAFK=GD.FK_GENERIC_HEADPAR
									AND CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
									AND C.CUST_ID = CC.FK_CUSTOMERCUST_ID
									AND GD.LATIN_DESC = C.PROFESSION ),1)),
                  CRD_TYPE.REPORTING_DATE
					FROM LNS_CRD_CUST_DATA C ,BOT_11_INDIVIDUAL BOT11
					where BOT11.CUSTOMERCODE = C.CUST_ID
						AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
 
				-- BOT_60_INDADDRESSES
					Insert into BOT_60_INDADDRESSES (FK_INDIVIDUAL, REPORTING_DATE )
						select BOT11.INDIVIDUAL_ID , CRD_TYPE.REPORTING_DATE
						FROM LNS_CRD_CUST_DATA C , BOT_11_INDIVIDUAL BOT11
						where BOT11.CUSTOMERCODE = C.CUST_ID
							AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
        -- BOT_64_EMPLOYER
					Insert into BOT_64_EMPLOYER (FK_INDADDRESSES,COUNTRY,DISTRICT,HOUSENUMBER,
													          	 REGION,STREETWARD,POBOXNUMBER,CITYTOWNVILLAGE, REPORTING_DATE)
						select bot60.INDADDRESSES_ID,
									trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
				    			trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
							            	from GENERIC_DETAIL GD1
								            where GD1.PARAMETER_TYPE = 'ADDDI'
								              AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
								trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end ))
                , 1 , --REGION,
								trim(STREET_WARD) , trim(zip_code), trim(D.CITY), CRD_TYPE.REPORTING_DATE
						from LNS_CRD_CUST_ADDRESS d ,BOT_60_INDADDRESSES bot60, BOT_11_INDIVIDUAL bot11
						where d.CUST_ID =  CRD_TYPE.CUST_ID
							and bot11.CUSTOMERCODE = d.CUST_ID
							and  bot60.FK_INDIVIDUAL = bot11.INDIVIDUAL_ID
							and d.ADDRESS_TYPE = 'Employer';
	
        -- BOT_67_POSTAL
					Insert into BOT_67_POSTAL (FK_INDADDRESSES,COUNTRY,DISTRICT,HOUSENUMBER,
													        	REGION,STREETWARD,POBOXNUMBER,CITYTOWNVILLAGE, REPORTING_DATE)
						select bot60.INDADDRESSES_ID,
									trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
    							trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
    								from GENERIC_DETAIL GD1
    								where GD1.PARAMETER_TYPE = 'ADDDI'
    								AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
								  trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end )), 1 , --REGION,
							  	trim(STREET_WARD) , trim(zip_code), trim(D.CITY), trim(CRD_TYPE.REPORTING_DATE)
						from LNS_CRD_CUST_ADDRESS d ,BOT_60_INDADDRESSES bot60, BOT_11_INDIVIDUAL bot11
						where d.CUST_ID =  CRD_TYPE.CUST_ID
							and bot11.CUSTOMERCODE = d.CUST_ID
							and  bot60.FK_INDIVIDUAL = bot11.INDIVIDUAL_ID
							and d.ADDRESS_TYPE = 'Postal';
				-- BOT_65_PERMRESIDENCE
					Insert into BOT_65_PERMRESIDENCE (FK_INDADDRESSES,COUNTRY,DISTRICT,HOUSENUMBER,
														REGION,STREETWARD,POBOXNUMBER,CITYTOWNVILLAGE, REPORTING_DATE)
						select bot60.INDADDRESSES_ID,
									trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
							trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
								from GENERIC_DETAIL GD1
								where GD1.PARAMETER_TYPE = 'ADDDI'
								AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
								trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end )), 1 , --REGION,
								trim(STREET_WARD) , trim(zip_code), trim(D.CITY), CRD_TYPE.REPORTING_DATE
						from LNS_CRD_CUST_ADDRESS d ,BOT_60_INDADDRESSES bot60, BOT_11_INDIVIDUAL bot11
						where d.CUST_ID =  CRD_TYPE.CUST_ID
							and bot11.CUSTOMERCODE = d.CUST_ID
							and  bot60.FK_INDIVIDUAL = bot11.INDIVIDUAL_ID
							and d.ADDRESS_TYPE = 'Permanent residence';
				-- BOT_66_PHYSICAL
					Insert into BOT_66_PHYSICAL (FK_INDADDRESSES,COUNTRY,DISTRICT,HOUSENUMBER,
												            	REGION,STREETWARD,POBOXNUMBER,CITYTOWNVILLAGE, REPORTING_DATE)
						select bot60.INDADDRESSES_ID,
									trim(NVL(( select (case when GD.SERIAL_NUM = 0 then 218 else  GD.LATIN_DESC  end)
									from GENERIC_DETAIL GD
									where GD.SHORT_DESCRIPTION = d.country
                    AND GD.PARAMETER_TYPE = 'CRDCN'  ), 218)), --country
						    	trim(NVL((select (case when GD1.SERIAL_NUM = 0 then 1 else  GD1.SERIAL_NUM  end)
						    		from GENERIC_DETAIL GD1
						    		where GD1.PARAMETER_TYPE = 'ADDDI'
						      		AND GD1.DESCRIPTION = d.DISTRICT), 1)) , --district
								trim((case when HOUSE_NUM = '' then  null else HOUSE_NUM end )), 1 , --REGION,
								trim(STREET_WARD) , trim(zip_code), trim(D.CITY), CRD_TYPE.REPORTING_DATE
						from LNS_CRD_CUST_ADDRESS d ,BOT_60_INDADDRESSES bot60, BOT_11_INDIVIDUAL bot11
						where d.CUST_ID =  CRD_TYPE.CUST_ID
							and bot11.CUSTOMERCODE = d.CUST_ID
							and  bot60.FK_INDIVIDUAL = bot11.INDIVIDUAL_ID
							and d.ADDRESS_TYPE = 'Physical';
				-- BOT_61_INDCONTACTS
					Insert into BOT_61_INDCONTACTS (FK_INDIVIDUAL,CELLULARPHONE,EMAIL,FAX,FIXEDLINE,WEBPAGE, REPORTING_DATE)
						select bot11.INDIVIDUAL_ID,trim((case when C.CELLULAR_PHONE = '' then  null else C.CELLULAR_PHONE end )),
            trim((case when C.EMAIL = '' then  null else C.EMAIL end )),trim((case when C.FAX = '' then  null else C.FAX end )),trim((case when C.FIXED_LINE = '' then  null else C.FIXED_LINE end )),
          trim((case when C.WEBPAGE = '' then  null else C.WEBPAGE end )),
                  CRD_TYPE.REPORTING_DATE
						from LNS_CRD_CUST_DATA C , BOT_11_INDIVIDUAL bot11
						where C.CUST_ID = CRD_TYPE.CUST_ID
							and c.cust_id = bot11.customercode  ;
				IF CRD_TYPE.MONTHLY_EXP_AMN <> 0 OR CRD_TYPE.MONTHLY_EXP_AMN IS NOT NULL THEN
					 -- BOT_69_EXPENDITURES
					 Insert into BOT_69_EXPENDITURES (FK_PERSONALDATA,VALUE,CURRENCY, REPORTING_DATE)
					   SELECT BOT63.PERSONALDATA_ID ,
							   trim(C.MONTHLY_EXP_AMN),
							   trim(NVL((SELECT ID_CURRENCY FROM CURRENCY  WHERE SHORT_DESCR = C.MONTHLY_EXP_CUR ), 147)),
                 CRD_TYPE.REPORTING_DATE
					   FROM LNS_CRD_CUST_DATA C ,
			    				BOT_11_INDIVIDUAL BOT11,
						      BOT_63_PERSONALDATA BOT63
						where BOT11.CUSTOMERCODE = C.CUST_ID
						 AND BOT11.INDIVIDUAL_ID = BOT63.FK_INDIVIDUAL
						 AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
				END IF;
				IF CRD_TYPE.MONTHLY_INC_AMN <> 0 OR CRD_TYPE.MONTHLY_INC_AMN IS NOT NULL THEN
					 -- BOT_49_MONTHLYINCOME
					 Insert into BOT_49_MONTHLYINCOME (FK_PERSONALDATA,VALUE,CURRENCY,REPORTING_DATE)
					   SELECT BOT63.PERSONALDATA_ID ,
							   trim(C.MONTHLY_INC_AMN),
							   trim(NVL((SELECT ID_CURRENCY FROM CURRENCY  WHERE SHORT_DESCR = C.MONTHLY_INC_CUR ), 147)),
                 CRD_TYPE.REPORTING_DATE
					   FROM LNS_CRD_CUST_DATA C ,
							    BOT_11_INDIVIDUAL BOT11,
							    BOT_63_PERSONALDATA BOT63
						   where BOT11.CUSTOMERCODE = C.CUST_ID
			    			 AND BOT11.INDIVIDUAL_ID = BOT63.FK_INDIVIDUAL
				    		 AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
				END IF;
				-- BOT_45_SPOUSEFULLNAME
				IF CRD_TYPE.SPOUSE_FULLNAME IS NOT NULL AND CRD_TYPE.SPOUSE_FULLNAME <> '' THEN
					 Insert into BOT_45_SPOUSEFULLNAME (SPOUSEFULLNAME, REPORTING_DATE)
              values (CRD_TYPE.SPOUSE_FULLNAME, CRD_TYPE.REPORTING_DATE);
	
          -- BOT_34_PERSONSPOUSE
					 Insert into BOT_34_PERSONSPOUSE (FK_PERSONALDATA,KEY,FK_BOT_45_SPOUSEFULLNAME, REPORTING_DATE)
					   SELECT BOT63.PERSONALDATA_ID,
							 (SELECT nvl(MAX(KEY), 0) + 1
								FROM BOT_34_PERSONSPOUSE  BOT34
							   WHERE BOT34.FK_PERSONALDATA = BOT63.PERSONALDATA_ID
							  ),
							  trim(BOT45.SPOUSEFULLNAME_ID) ,CRD_TYPE.REPORTING_DATE
							FROM LNS_CRD_CUST_DATA C ,
								 BOT_11_INDIVIDUAL BOT11,
								 BOT_63_PERSONALDATA BOT63,
								 BOT_45_SPOUSEFULLNAME BOT45
						  where BOT11.CUSTOMERCODE = C.CUST_ID
							AND BOT11.INDIVIDUAL_ID = BOT63.FK_INDIVIDUAL
							AND BOT45.SPOUSEFULLNAME = CRD_TYPE.SPOUSE_FULLNAME
							AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
				END IF;
				-- BOT_68_BIRTHDATA
				IF CRD_TYPE.DATE_OF_BIRTH IS NOT NULL THEN
				  Insert into BOT_68_BIRTHDATA (FK_PERSONALDATA,BIRTHDATE,COUNTRY,DISTRICT, REPORTING_DATE)
					SELECT BOT63.PERSONALDATA_ID, C.DATE_OF_BIRTH,
						   trim(NVL((select (CASE WHEN GD.LATIN_DESC = 0 THEN 218 ELSE gd.LATIN_DESC END )
               						 from customer_category cc, generic_Detail gd
								where CC.FK_CATEGORYCATEGOR='BCOUNTRY'
								  and CC.FK_GENERIC_DETAFK='CRDCN'
								  and CC.FK_GENERIC_DETASER=GD.SERIAL_NUM
								  AND CC.FK_CUSTOMERCUST_ID = C.CUST_ID
								  AND gd.SHORT_DESCRIPTION = C.COUNTRY_OF_BIRTH), 218)), --COUNTRY OF BIRTH
							 1, CRD_TYPE.REPORTING_DATE
					  FROM LNS_CRD_CUST_DATA C ,
						   BOT_11_INDIVIDUAL BOT11,
						   BOT_63_PERSONALDATA BOT63
					   where BOT11.CUSTOMERCODE = C.CUST_ID
					 AND BOT11.INDIVIDUAL_ID = BOT63.FK_INDIVIDUAL
					 AND C.CUST_ID =  CRD_TYPE.CUST_ID ;
				END IF;
				-- BOT_62_IDENTIFICATIONS
				IF ( (CRD_TYPE.DRIVING_LICENCE_ID IS NOT NULL AND CRD_TYPE.DRIVING_LICENCE_ID <> '' ) OR
				  	 (CRD_TYPE.NATIONAL_ID  IS NOT NULL AND CRD_TYPE.NATIONAL_ID <> '' ) OR
				  	 (CRD_TYPE.PERMISSION_ID IS NOT NULL  AND CRD_TYPE.PERMISSION_ID <> '' ) OR
					   (CRD_TYPE.VOTER_REG_ID IS NOT NULL AND CRD_TYPE.VOTER_REG_ID <> '' ) OR
				  	 (CRD_TYPE.WARD_ID IS NOT NULL AND CRD_TYPE.WARD_ID <> '' ) OR
					   (CRD_TYPE.ZANZIBAR_ID IS NOT NULL  AND CRD_TYPE.ZANZIBAR_ID <> '' ) OR
             (CRD_TYPE.PASSPORT_ID IS NOT NULL   AND CRD_TYPE.PASSPORT_ID <> '' )
            ) THEN
	
					 Insert into BOT_62_IDENTIFICATIONS (FK_INDIVIDUAL, REPORTING_DATE )
					  SELECT BOT11.INDIVIDUAL_ID, CRD_TYPE.REPORTING_DATE
						FROM LNS_CRD_CUST_DATA C ,
							    BOT_11_INDIVIDUAL BOT11
						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
 
          IF CRD_TYPE.PASSPORT_ID IS NOT NULL AND CRD_TYPE.PASSPORT_ID <> '' THEN
            --BOT_68_PASSPORT
             Insert into BOT_68_PASSPORT (FK_IDENTIFICATIONS,NUMBEROFPASSPORT,
  												  DATEOFEXPIRATION,DATEOFISSUANCE,
  												  ISSUANCELOCATION,ISSUEDBY, REPORTING_DATE)
  					   SELECT BOT62.IDENTIFICATIONS_ID,trim((case when C.PASSPORT_ID = '' then  null else C.PASSPORT_ID end )) ,
  							    trim(C.P_EXPIRE_DT) , trim(C.P_ISSUE_DT) ,trim(C.P_ISSUE_LOCAT) ,
                    trim((case when C.P_ISSUE_AUTH is null or C.P_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.P_ISSUE_AUTH end ))
  							    , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
 
				  IF CRD_TYPE.DRIVING_LICENCE_ID IS NOT NULL AND CRD_TYPE.DRIVING_LICENCE_ID <> '' THEN
            --BOT_71_DRIVINGLICENSE
             Insert into BOT_71_DRIVINGLICENSE (FK_IDENTIFICATIONS,NUMBEROFDRIVINGLICENSE,
  												  DATEOFEXPIRATION,DATEOFISSUANCE,
  												  ISSUANCELOCATION,ISSUEDBY, REPORTING_DATE)
  					   SELECT BOT62.IDENTIFICATIONS_ID,trim((case when C.DRIVING_LICENCE_ID = '' then  null else C.DRIVING_LICENCE_ID end )),
  							    trim(C.DL_EXPIRE_DT) , trim(C.DL_ISSUE_DT) ,trim(C.DL_ISSUE_LOCAT) ,
                    trim((case when C.DL_ISSUE_AUTH is null or C.DL_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.DL_ISSUE_AUTH end ))
  							    , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
	
          IF CRD_TYPE.NATIONAL_ID IS NOT NULL AND CRD_TYPE.NATIONAL_ID <> '' THEN
            -- BOT_73_NATIONALID
  					 Insert into BOT_73_NATIONALID (FK_IDENTIFICATIONS,NUMBEROFNATIONALID,
  												  DATEOFEXPIRATION,DATEOFISSUANCE,
  												  ISSUANCELOCATION,ISSUEDBY, REPORTING_DATE )
  					   SELECT BOT62.IDENTIFICATIONS_ID,trim((case when C.NATIONAL_ID = '' then  null else C.NATIONAL_ID end )),
  							    trim(C.NI_EXPIRE_DT) , trim(C.NI_ISSUE_DT) , trim(C.NI_ISSUE_LOCAT) ,
                    trim((case when C.NI_ISSUE_AUTH is null or C.NI_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.NI_ISSUE_AUTH end ))
  							    , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
 
				  IF CRD_TYPE.PERMISSION_ID IS NOT NULL AND CRD_TYPE.PERMISSION_ID <> '' THEN
          --BOT_74_PERMITRESIDENCE
  					 Insert into BOT_74_PERMITRESIDENCE (FK_IDENTIFICATIONS,NUMBEROFPERMISSION,
  												                       DATEOFEXPIRATION,DATEOFISSUANCE,
  												                       ISSUANCELOCATION,ISSUEDBY, REPORTING_DATE )
  					   SELECT BOT62.IDENTIFICATIONS_ID, trim(C.PERMISSION_ID) ,
  							      trim(C.PR_EXPIRE_DT) , trim(C.PR_ISSUE_DT) , trim(C.PR_ISSUE_LOC) ,
                      trim((case when C.PR_ISSUE_AUTH is null or C.PR_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.PR_ISSUE_AUTH end ))
  							      , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
 
				  IF CRD_TYPE.VOTER_REG_ID IS NOT NULL AND CRD_TYPE.VOTER_REG_ID <> '' THEN
            -- BOT_75_VOTERREGNUMBER
  					 Insert into BOT_75_VOTERREGNUMBER (FK_IDENTIFICATIONS,NUMBEROFVOTERREGISTRATION,
  												                      DATEOFEXPIRATION,DATEOFISSUANCE,
  											                    	  ISSUANCELOCATION,ISSUEDBY, REPORTING_DATE)
  					   SELECT BOT62.IDENTIFICATIONS_ID, trim(case when C.VOTER_REG_ID = '' then null else C.VOTER_REG_ID end) ,
  							      trim(C.VR_EXPIRE_DT) , trim(C.VR_ISSUE_DT) , trim(C.VR_ISSUE_LOC) ,
                      trim((case when C.VR_ISSUE_AUTH is null or C.VR_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.VR_ISSUE_AUTH end ))
  							      , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
 
				  IF CRD_TYPE.WARD_ID IS NOT NULL  AND CRD_TYPE.WARD_ID <> '' THEN
             -- BOT_76_WARDID
  					 Insert into BOT_76_WARDID (FK_IDENTIFICATIONS,NUMBEROFWARDID,
  									        	      		  DATEOFEXPIRATION,DATEOFISSUANCE,
  												                ISSUANCELOCATION,ISSUEDBY,REPORTING_DATE)
  					   SELECT BOT62.IDENTIFICATIONS_ID, trim(C.WARD_ID) ,
  							      trim(C.WI_EXPIRE_DT) , trim(C.WI_ISSUE_DT) , trim(C.WI_ISSUE_LOC) ,
                      trim((case when C.WI_ISSUE_AUTH is null or C.WI_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.WI_ISSUE_AUTH end ))
  							      , CRD_TYPE.REPORTING_DATE
  						 FROM LNS_CRD_CUST_DATA C ,
  							BOT_11_INDIVIDUAL BOT11,
  							BOT_62_IDENTIFICATIONS BOT62
  						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
  						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
  						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
 
				  IF CRD_TYPE.ZANZIBAR_ID IS NOT NULL AND CRD_TYPE.ZANZIBAR_ID <> '' THEN
             -- BOT_77_ZANZIBARID
						 Insert into BOT_77_ZANZIBARID (FK_IDENTIFICATIONS,NUMBEROFZANZIBARID,
											                  	  DATEOFEXPIRATION,DATEOFISSUANCE,
											                  	  ISSUANCELOCATION,ISSUEDBY , REPORTING_DATE)
					   SELECT BOT62.IDENTIFICATIONS_ID,trim(case when C.ZANZIBAR_ID = '' then null else C.ZANZIBAR_ID end),
							      trim(C.ZI_EXPIRE_DT), trim(C.ZI_ISSUE_DT), trim(C.ZI_ISSUE_LOC),
                    trim((case when C.ZI_ISSUE_AUTH is null or C.ZI_ISSUE_AUTH = ''
                          then 'Unknown Authority' else C.ZI_ISSUE_AUTH end ))
                    , CRD_TYPE.REPORTING_DATE
						 FROM LNS_CRD_CUST_DATA C ,
							BOT_11_INDIVIDUAL BOT11,
							BOT_62_IDENTIFICATIONS BOT62
						 WHERE BOT11.CUSTOMERCODE = C.CUST_ID
						 AND BOT62.FK_INDIVIDUAL = BOT11.INDIVIDUAL_ID
						 AND C.CUST_ID =  CRD_TYPE.CUST_ID;
				  END IF;
				END IF;
    END IF;
  END FOR;
  ----------------------------------------------------------------------------
	-------------------------- CREDITS DATA START ------------------------------
	----------------------------------------------------------------------------
  FOR CRD_DATA AS
   (SELECT LOAN_CODE, REPORTING_DATE, ADDITIONAL_FEE_SUM , ADDITIONAL_FEE_PAID ,
           trim(substr(p.ACCOUNT_NUMBER, 1, 32)) AS INST
     FROM LNS_CRD_LOAN_DATA L, PROFITS_ACCOUNT P
    WHERE L.LOAN_CODE = P.ACCOUNT_NUMBER
    ORDER BY LOAN_CODE ASC)
  DO
  -- BOT_1_COMMAND
		Insert into BOT_1_COMMAND (IDENTIFIER, loan_code, reporting_date)
			VALUES (CRD_DATA.INST , CRD_DATA.LOAN_CODE , CRD_DATA.REPORTING_DATE) ;
 
		-- BOT_90_STORHEADER
		Insert into BOT_90_STORHEADER (SOURCE, STORETO, IDENTIFIER, LOAN_CODE)
			values ('MWALIMU', CRD_DATA.REPORTING_DATE ,'INST-' || CRD_DATA.INST  , CRD_DATA.LOAN_CODE);
 
		-- BOT_70_FEESANDPENALTIES
		IF CRD_DATA.ADDITIONAL_FEE_SUM <> 0  OR CRD_DATA.ADDITIONAL_FEE_PAID <> 0 THEN
			Insert into BOT_70_FEESANDPENALTIES (ADDITIONALFEESPAID,ADDITIONALFEESSUM, loan_code)
				values (CRD_DATA.ADDITIONAL_FEE_PAID, CRD_DATA.ADDITIONAL_FEE_SUM, CRD_DATA.LOAN_CODE );
		END IF;
 
		-- BOT_4_STORINSTALMENT
		Insert into BOT_4_STORINSTALMENT (FK_COMMAND, FK_BOT_90_STORHEADER)
			values ((select trim(max(COMMAND_ID)) from BOT_1_COMMAND),
				    	(select trim(max(STORHEADER_ID)) from BOT_90_STORHEADER WHERE LOAN_CODE = CRD_DATA.LOAN_CODE)
				    );
 
		-- NO DISPUTE INFORMATION IN LOAN TABLES
		-- BOT_91_DISPUTE
 
		-- BOT_72_CONTRACTDATES
     Insert into BOT_72_CONTRACTDATES ( EXPECTEDEND	, LASTPAYMENT	, REALEND	, START,
                                        LOAN_CODE , REPORTING_DATE)
			SELECT  trim(LC.LOAN_END_DATE),
              trim((case when LC.LAST_PAYMENT_DATE = to_date('01/01/0001', 'dd/mm/yyyy')
                         then null else LC.LAST_PAYMENT_DATE end )),
              trim(LC.REAL_LOAN_END_DATE), trim(LC.LOAN_START_DATE),
               CRD_DATA.LOAN_CODE , CRD_DATA.REPORTING_DATE
            FROM  LNS_CRD_LOAN_DATA LC
           WHERE LC.LOAN_CODE = CRD_DATA.LOAN_CODE
             AND LC.REPORTING_DATE = CRD_DATA.REPORTING_DATE;
 
		-- BOT_17_INSTALMENT
		Insert into BOT_17_INSTALMENT (FK_STORINSTALMENT,INSTALMENTCOUNT,INSTALMENTTYPE,
				                          METHODOFPAYMENT,OUTSTANDINGAMOUNT,OUTSTANDINGINSTALMENTCOUNT,
								                  OVERDUEINSTALMENTCOUNT,PERIODICITYOFPAYMENTS,STANDARDINSTALMENTAMOUNT,
								                  TYPEOFINSTALMENTLOAN,CURRENCYOFLOAN,ECONOMICSECTOR,
								                  NEGATIVESTATUSOFLOAN,PASTDUEAMOUNT,PASTDUEDAYS,
								                  PHASEOFLOAN,PURPOSEOFLOAN,RESCHEDULEDLOAN,
								                  TOTALLOANAMOUNT,FK_BOT_91_DISPUTE,FK_BOT_70_FEESANDPENALTIES,
                                  LOAN_CODE , REPORTING_DATE, FK_BOT_72_CONTRACTDATES)
			SELECT (select max(STORINSTALMENT_ID) from BOT_4_STORINSTALMENT),
					    trim(LC.TOTAL_INSTALLMENTS),
					    trim((case when lc.INSTALLMENT_TYPE like 'F%' then 1 else (case when lc.INSTALLMENT_TYPE like 'V%' then 2 else 1 end) end)),
					    1,trim(lc.OVERDUE_AMOUNT),trim(lc.OVERDUE_REMAIN_INSTAL), trim(lc.OVERDUE_INSTAL_COUNT),
				    	trim((CASE WHEN Lc.PAYMENTS_PERIODICITY= 'MonthlyInstalments30Days' THEN 3
                         WHEN LC.PAYMENTS_PERIODICITY= 'BimonthlyInstalments60Days' THEN 4
                         WHEN LC.PAYMENTS_PERIODICITY= 'QuarterlyInstalments90Days' THEN 5
                         WHEN LC.PAYMENTS_PERIODICITY= 'FourMonthInstalments120Days' THEN 6
                         WHEN LC.PAYMENTS_PERIODICITY= 'FiveMonthInstalments150Days' THEN 7
                         WHEN LC.PAYMENTS_PERIODICITY= 'SixMonthInstalments180Days' THEN 8
                         WHEN LC.PAYMENTS_PERIODICITY= 'AnnualInstalments360Days' THEN 9
                         ELSE
                         CASE WHEN LC.PAYMENTS_PERIODICITY= 'AtTheFinalDayOfThePeriodOfContract'  THEN 1
                         ELSE 10 END
					          END)), trim(lc.INSTALLMENT_AMOUNT),
					    trim((CASE WHEN Lc.LOAN_TYPE LIKE 'Cons%' THEN 1
					              WHEN Lc.LOAN_TYPE LIKE 'Bus%' THEN 2
                        WHEN Lc.LOAN_TYPE LIKE 'Mort%' THEN 3
                        WHEN Lc.LOAN_TYPE LIKE 'Leas%Fin%' THEN 4
                        WHEN Lc.LOAN_TYPE LIKE 'Leas%Oper%' THEN 5
                        ELSE 6
					          END )),
					    trim((SELECT (CASE WHEN ID_CURRENCY = 22 THEN 147 ELSE ID_CURRENCY END)
		              		FROM CURRENCY CC
				            	WHERE CC.SHORT_DESCR = LC.LOAN_CURRENCY )) ,
					    trim((SELECT (CASE WHEN GD.SERIAL_NUM = 0 THEN 16 ELSE GD.SERIAL_NUM END)
					            FROM GENERIC_DETAIL GD
					            WHERE GD.PARAMETER_TYPE = 'FINSC'
				              	AND GD.DESCRIPTION = LC.ECONOMIC_SECTOR )),
				      trim((CASE WHEN Lc.LOAN_NEGATIVE_STATUS LIKE 'NoNeg%' THEN 1
					            	WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Unautho%' THEN 2
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Blo%' THEN 3
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Cancel%' THEN 4
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Insura%' THEN 5
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'FraudTo%' THEN 6
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Assign%' THEN 7
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'LoanWr%' THEN 8
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'Increa%' THEN 9
            						WHEN LC.LOAN_NEGATIVE_STATUS LIKE 'LoanTransf%' THEN 10
            						ELSE 3
				          	END)), trim(PAST_DUE_AMOUNT), trim(PAST_DUE_DAYS) ,trim(TO_NUMBER(LOAN_PHASE)),
				      trim((CASE WHEN Lc.LOAN_PURPOSE LIKE 'Constr%' THEN 1
            						WHEN Lc.LOAN_PURPOSE LIKE 'Develop%' THEN 2
            						WHEN Lc.LOAN_PURPOSE LIKE 'W%' THEN 3
            						WHEN Lc.LOAN_PURPOSE LIKE 'Purc%B%' THEN 4
            						WHEN Lc.LOAN_PURPOSE LIKE 'Purc%Pe%' THEN 5
            						WHEN Lc.LOAN_PURPOSE LIKE 'Repa%' THEN 6
            						WHEN Lc.LOAN_PURPOSE LIKE 'Synd%' THEN 7
            						ELSE 8
				            END )),
              trim((CASE WHEN LC.RECHEDULER_LOAN = 'FALSE' THEN 2 ELSE 1 END  )),
              trim(LC.LOAN_AMOUNT), NULL ,
              trim((SELECT MAX(FEESANDPENALTIES_ID ) FROM BOT_70_FEESANDPENALTIES WHERE LOAN_CODE = CRD_DATA.LOAN_CODE)),
              CRD_DATA.LOAN_CODE , CRD_DATA.REPORTING_DATE ,
              trim((SELECT MAX(CONTRACTDATES_ID )
                      FROM BOT_72_CONTRACTDATES
                     WHERE LOAN_CODE = CRD_DATA.LOAN_CODE
                        and REPORTING_DATE = crd_data.REPORTING_DATE))
            FROM  LNS_CRD_LOAN_DATA LC
           WHERE LC.LOAN_CODE = CRD_DATA.LOAN_CODE
             AND LC.REPORTING_DATE = CRD_DATA.REPORTING_DATE;
  END FOR;
 
  ----------------------------------------------------------------------------
	-------------------------- CREDITS DATA END --------------------------------
	----------------------------------------------------------------------------
  ----------------------------------------------------------------------------
	-------------------------- CREDITS CUSTOMER SUB DATA START -----------------
	----------------------------------------------------------------------------
    FOR CRD_CUST AS
     (SELECT ls.CUST_ID, C.CUST_TYPE, LS.LOAN_CODE, LS.REPORTING_DATE ,
             trim(substr(p.ACCOUNT_NUMBER, 1, 32)) AS INST
        FROM LNS_CRD_CUST_SUB_DATA ls,
             LNS_CRD_LOAN_DATA L,
             LNS_CRD_CUST_DATA C,
             PROFITS_ACCOUNT P
       where LS.LOAN_CODE = L.LOAN_CODE
         AND P.ACCOUNT_NUMBER = L.LOAN_CODE
         AND LS.REPORTING_DATE = L.REPORTING_DATE
         AND ls.CUST_ID = C.CUST_ID
       ORDER BY ls.CUST_ID ASC  )
    DO
      IF CRD_CUST.CUST_TYPE = 2 THEN
		    -- company
        -- BOT_2_SUBJECTCHOICE
          Insert into BOT_2_SUBJECTCHOICE (FK_BOT_10_COMPANY, cust_id, LOAN_CODE, REPORTING_DATE)
            SELECT BOT10.COMPANY_ID, trim(CRD_CUST.CUST_ID),  CRD_CUST.LOAN_CODE, CRD_CUST.REPORTING_DATE
              FROM BOT_10_COMPANY BOT10
             WHERE BOT10.CUSTOMERCODE = CRD_CUST.CUST_ID;
            -- BOT_30_CONNECTEDSUBJECT
            Insert into BOT_30_CONNECTEDSUBJECT (COLLATERALTYPE,COLLATERALVALUE,COMMENT1,
                                                 ROLEOFCLIENT,FK_BOT_2_SUBJECTCHOICE,
                                                 CUST_ID, LOAN_CODE, REPORTING_DATE )
              select  NULL, trim(SUB1.collateral_value) , NULL,
                      trim((CASE WHEN (SUB1.CLIENT_ROLE like 'MainDe%' or
                                  SUB1.CLIENT_ROLE like 'CoDeb%' ) THEN 1
                           ELSE 2 END)),
                      trim(NVL((SELECT BOT2.SUBJECTCHOICE_ID
                             FROM BOT_2_SUBJECTCHOICE BOT2
                            WHERE BOT2.CUST_ID = CRD_CUST.CUST_ID
				                       AND BOT2.LOAN_CODE = CRD_CUST.LOAN_CODE
				                       AND BOT2.REPORTING_DATE = CRD_CUST.REPORTING_DATE),1)),
                      CRD_CUST.CUST_ID,
                      CRD_CUST.LOAN_CODE,
                      CRD_CUST.REPORTING_DATE
                from LNS_CRD_CUST_SUB_DATA SUB1
               WHERE SUB1.CUST_ID = CRD_CUST.CUST_ID
				         AND SUB1.LOAN_CODE = CRD_CUST.LOAN_CODE
				         AND SUB1.REPORTING_DATE = CRD_CUST.REPORTING_DATE;
 
            -- BOT_36_INSTCONNSUBJECT
      			Insert into BOT_36_INSTCONNSUBJECT (FK_INSTALMENT,KEY,FK_BOT_30_CONNECTEDSUBJECT,
                                                 CUST_ID, LOAN_CODE, REPORTING_DATE) values
        			((select max(INSTALMENT_ID)
                  from BOT_17_Instalment
                 WHERE LOAN_CODE = CRD_CUST.LOAN_CODE
                   AND REPORTING_DATE = CRD_CUST.REPORTING_DATE),
        			 CRD_CUST.INST,
        			 (select max(CONNECTEDSUBJECT_ID)
                  from BOT_30_CONNECTEDSUBJECT
                 WHERE LOAN_CODE = CRD_CUST.LOAN_CODE
                   AND REPORTING_DATE = CRD_CUST.REPORTING_DATE
                   AND CUST_ID = CRD_CUST.CUST_ID),
               CRD_CUST.CUST_ID,
               CRD_CUST.LOAN_CODE,
               CRD_CUST.REPORTING_DATE
               );
		  ELSE
    			-- individual
    			-- BOT_2_SUBJECTCHOICE
    			Insert into BOT_2_SUBJECTCHOICE (FK_BOT_11_INDIVIDUAL, CUST_ID, LOAN_CODE, REPORTING_DATE)
    			  SELECT BOT11.INDIVIDUAL_ID, CRD_CUST.CUST_ID,  CRD_CUST.LOAN_CODE, CRD_CUST.REPORTING_DATE
              FROM BOT_11_INDIVIDUAL BOT11
             WHERE BOT11.CUSTOMERCODE = CRD_CUST.CUST_ID;
    			-- BOT_30_CONNECTEDSUBJECT
          Insert into BOT_30_CONNECTEDSUBJECT (COLLATERALTYPE,COLLATERALVALUE,COMMENT1,
                                               ROLEOFCLIENT,FK_BOT_2_SUBJECTCHOICE,
                                               CUST_ID, LOAN_CODE, REPORTING_DATE)
              select  NULL, trim(SUB1.collateral_value) , NULL,
                      trim((CASE WHEN (SUB1.CLIENT_ROLE like 'MainDe%' or
                                  SUB1.CLIENT_ROLE like 'CoDeb%' ) THEN 1
                           ELSE 2 END)),
                      trim(NVL((SELECT BOT2.SUBJECTCHOICE_ID
                             FROM BOT_2_SUBJECTCHOICE BOT2
                            WHERE BOT2.CUST_ID = CRD_CUST.CUST_ID
				                       AND BOT2.LOAN_CODE = CRD_CUST.LOAN_CODE
				                       AND BOT2.REPORTING_DATE = CRD_CUST.REPORTING_DATE),1)),
                      CRD_CUST.CUST_ID,
                      CRD_CUST.LOAN_CODE,
                      CRD_CUST.REPORTING_DATE
                from LNS_CRD_CUST_SUB_DATA SUB1
               WHERE SUB1.CUST_ID = CRD_CUST.CUST_ID
				         AND SUB1.LOAN_CODE = CRD_CUST.LOAN_CODE
				         AND SUB1.REPORTING_DATE = CRD_CUST.REPORTING_DATE;
	
			    -- BOT_36_INSTCONNSUBJECT
          Insert into BOT_36_INSTCONNSUBJECT (FK_INSTALMENT,KEY,FK_BOT_30_CONNECTEDSUBJECT,
                                              CUST_ID, LOAN_CODE, REPORTING_DATE  ) values
        			((select max(INSTALMENT_ID)
                  from BOT_17_Instalment
                 WHERE LOAN_CODE = CRD_CUST.LOAN_CODE
                   AND REPORTING_DATE = CRD_CUST.REPORTING_DATE),
        			 CRD_CUST.INST,
        			 (select max(CONNECTEDSUBJECT_ID)
                  from BOT_30_CONNECTEDSUBJECT
                 WHERE LOAN_CODE = CRD_CUST.LOAN_CODE
                   AND REPORTING_DATE = CRD_CUST.REPORTING_DATE
                   AND CUST_ID = CRD_CUST.CUST_ID),
               CRD_CUST.CUST_ID,
               CRD_CUST.LOAN_CODE,
               CRD_CUST.REPORTING_DATE
              );
      END IF;
  END FOR;
  ------------------------------------------------------------------------------
	----------------------- CREDITS CUSTOMER SUB DATA END ------------------------
	------------------------------------------------------------------------------
  --------------------------------------------------------------------------------
	---------------------------- COLLATERAL SUB DATA START -------------------------
	--------------------------------------------------------------------------------
    FOR CRD_COLL AS
     (SELECT P.CUST_ID, LS.LOAN_CODE, LS.REPORTING_DATE ,
             trim(substr(P.ACCOUNT_NUMBER, 1, 32)) AS INST
        FROM LNS_CRD_COLL_SUB_DATA ls,
             LNS_CRD_LOAN_DATA L,
             PROFITS_ACCOUNT P
       where  LS.LOAN_CODE = L.LOAN_CODE
         AND P.ACCOUNT_NUMBER = LS.LOAN_CODE
         AND LS.REPORTING_DATE = L.REPORTING_DATE
       GROUP BY P.CUST_ID, LS.LOAN_CODE, LS.REPORTING_DATE ,
       trim(substr(P.ACCOUNT_NUMBER, 1, 32)) )
    DO
      --collaterals
      Insert into BOT_43_COLLATERAL (COLLATERALTYPE,COLLATERALVALUE,COMMENT1,REGISTEREDCOLLATERAL,
                                      CUST_ID, LOAN_CODE, REPORTING_DATE, COLLATERAL_SN, COLL_TYPE,COLL_UNIT)
             SELECT trim((CASE WHEN SUB2.COLLATERAL_TYPE like 'Stoc%' THEN 1
                     WHEN SUB2.COLLATERAL_TYPE like 'Depo%' THEN 2
                     WHEN SUB2.COLLATERAL_TYPE like 'Salary%' THEN 3
                     WHEN SUB2.COLLATERAL_TYPE like 'Real%' THEN 4
                     WHEN SUB2.COLLATERAL_TYPE like 'Termin%' THEN 5
                     WHEN SUB2.COLLATERAL_TYPE like 'Equipm%' THEN 6
                     WHEN SUB2.COLLATERAL_TYPE like 'Governme%' THEN 7
                     WHEN SUB2.COLLATERAL_TYPE like 'Gol%' THEN 8
                     WHEN SUB2.COLLATERAL_TYPE like 'StateGua%' THEN 9
                     WHEN SUB2.COLLATERAL_TYPE like 'Motor%' THEN 10
                     ELSE 11 END)),
                 trim(SUB2.collateral_value) ,
                 NULL, 1,
                 CRD_COLL.CUST_ID,
                 CRD_COLL.LOAN_CODE,
                 CRD_COLL.REPORTING_DATE,
                 SUB2.COLLATERAL_SN,
                 sub2.COLL_TYPE, sub2.COLL_UNIT
          from LNS_CRD_COLL_SUB_DATA SUB2
          WHERE SUB2.LOAN_CODE = CRD_COLL.LOAN_CODE
		        AND SUB2.REPORTING_DATE = CRD_COLL.REPORTING_DATE;
       Insert into BOT_35_INSTCOLLATERAL (FK_INSTALMENT,KEY,FK_BOT_43_COLLATERAL,
                                          CUST_ID, LOAN_CODE, REPORTING_DATE)
        SELECT (select max(INSTALMENT_ID)
                   from BOT_17_Instalment
                  WHERE LOAN_CODE = CRD_COLL.LOAN_CODE
                     AND REPORTING_DATE = CRD_COLL.REPORTING_DATE),
                --CRD_COLL.INST,
                trim(to_chaR(BOT43.COLLATERAL_ID)),
                BOT43.COLLATERAL_ID
                , CRD_COLL.CUST_ID, CRD_COLL.LOAN_CODE, CRD_COLL.REPORTING_DATE
           from LNS_CRD_COLL_SUB_DATA SUB2,
                BOT_43_COLLATERAL BOT43
          WHERE SUB2.LOAN_CODE = CRD_COLL.LOAN_CODE
		        AND SUB2.REPORTING_DATE = CRD_COLL.REPORTING_DATE
            AND SUB2.LOAN_CODE = BOT43.LOAN_CODE
            AND SUB2.REPORTING_DATE = BOT43.REPORTING_DATE
            AND BOT43.CUST_ID = CRD_COLL.CUST_ID
            AND BOT43.COLLATERAL_SN = SUB2.COLLATERAL_SN
            and bot43.COLL_TYPE = sub2.COLL_TYPE
            and bot43.COLL_UNIT = sub2.COLL_UNIT;
    END FOR;
  ------------------------------------------------------------------------------
	---------------------------- COLLATERAL SUB DATA END -------------------------
	------------------------------------------------------------------------------
END;

