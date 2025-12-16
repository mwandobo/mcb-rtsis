create table CLC_INT_ADDRESSES
(
    POC_ID                       VARCHAR(32) not null,
    BP_ID                        VARCHAR(32) not null,
    POC_LOCTYPE                  SMALLINT    not null,
    POC_TYPE                     VARCHAR(3)  not null,
    POC_ADDR_LINE1               VARCHAR(100),
    POC_ADDRCOUNTRY              VARCHAR(100),
    POC_ADDRSTREETNAME           VARCHAR(100),
    POC_ADDRSTREETNO             VARCHAR(32),
    POC_ADDRREGIONNAME           VARCHAR(100),
    POC_ADDRNATIONALPOSTALCODE   VARCHAR(32),
    POC_ADDRCITYNAME             VARCHAR(150),
    POC_ADDRCOUNTYNAME           VARCHAR(32),
    POC_ADDRPOBOX                VARCHAR(32),
    POC_TELFCOUNTRYCODE          VARCHAR(5),
    POC_TELFAREACODE             VARCHAR(5),
    POC_TELFLINENUMBER           VARCHAR(10),
    POC_TELFEXTENSIONNUMBER      VARCHAR(10),
    POCTELFDESCRIPTION           VARCHAR(250),
    POC_TELFCOMMENT              VARCHAR(250),
    POC_TELFISFAX                CHAR(1),
    POC_EMAIL                    VARCHAR(250),
    POC_DATADATE                 DATE        not null,
    POC_EXPORTDATE               DATE        not null,
    TMSTAMP_CUST_ADDRESS         TIMESTAMP(6),
    POC_IS_COMMUNICATION_ADDRESS CHAR(1),
    constraint PK_CLC_INT_ADDRESS
        primary key (POC_ID, POC_EXPORTDATE)
);

comment on table CLC_INT_ADDRESSES is 'This table will contain telephones and addresses of customers kept in CLC_INT_CUSTOMER';

comment on column CLC_INT_ADDRESSES.POC_ID is 'Point of Contact (PoC) unique id -';

comment on column CLC_INT_ADDRESSES.BP_ID is 'Customer ID - CUST_ADDRESS.FK_CUSTOMERCUST_ID';

comment on column CLC_INT_ADDRESSES.POC_LOCTYPE is '1:Telephone /2:Address/3:Email - CUSTOMER.COMMUN_METHOD';

comment on column CLC_INT_ADDRESSES.POC_TYPE is 'Type of contact (Home, work, etc) - CUST_ADDRESS.ADDRESS_TYPE';

comment on column CLC_INT_ADDRESSES.POC_ADDR_LINE1 is 'Address Line 1 - CUST_ADDRESS.ADDRESS_1 +CUST_ADDRESS.CUST_ADDRESS_2 +SUBSTR (CUST_ADDRESS.CUST_ADDRESS_3, 0 ,20)"';

comment on column CLC_INT_ADDRESSES.POC_ADDRCOUNTRY is 'Country/ - "cust_address always has country one generic_detail customer_category for country(parameter CNTRY)"';

comment on column CLC_INT_ADDRESSES.POC_ADDRSTREETNAME is 'Street (it will be given along with the street number) -';

comment on column CLC_INT_ADDRESSES.POC_ADDRSTREETNO is 'Street Number -';

comment on column CLC_INT_ADDRESSES.POC_ADDRREGIONNAME is 'Region/ - CUST_ADDRESS.FKGD_HAS_AS_DISTRI';

comment on column CLC_INT_ADDRESSES.POC_ADDRNATIONALPOSTALCODE is 'Post Code - CUST_ADDRESS.ZIP_CODE';

comment on column CLC_INT_ADDRESSES.POC_ADDRCITYNAME is 'City - CUST_ADDRESS.CITY';

comment on column CLC_INT_ADDRESSES.POC_ADDRCOUNTYNAME is 'Area/ - CUST_ADDRESS. REGION';

comment on column CLC_INT_ADDRESSES.POC_ADDRPOBOX is 'PO BOX - CUST_ADDRESS.MAIL_BOX';

comment on column CLC_INT_ADDRESSES.POC_TELFCOUNTRYCODE is 'Telephone Country Code -';

comment on column CLC_INT_ADDRESSES.POC_TELFAREACODE is 'Area Code Prefix -';

comment on column CLC_INT_ADDRESSES.POC_TELFLINENUMBER is 'Telephone Number - CUST_ADDRESS.TELEPHONE';

comment on column CLC_INT_ADDRESSES.POC_TELFEXTENSIONNUMBER is 'Extension';

comment on column CLC_INT_ADDRESSES.POCTELFDESCRIPTION is 'Description -';

comment on column CLC_INT_ADDRESSES.POC_TELFCOMMENT is 'Comment - CUST_ADDRESS.ENTRY_COMMENTS';

comment on column CLC_INT_ADDRESSES.POC_TELFISFAX is 'Fax indication - 1=Yes if CUST_ADDRESS.FAX_NO is filled else 0';

comment on column CLC_INT_ADDRESSES.POC_EMAIL is 'Email - CUST_ADDRESS.E_MAIL';

comment on column CLC_INT_ADDRESSES.POC_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ADDRESSES.POC_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

