create table CLC_INT_ASSETS
(
    ASSET_ID                  VARCHAR(20) not null,
    TYPE                      VARCHAR(20),
    SHORTDESCR                VARCHAR(30),
    CIF                       VARCHAR(20) not null,
    ASSETVALUE                DECIMAL(12, 2),
    PROPERTYCATEGORY          CHAR(2),
    PROPERTYTYPE              CHAR(2),
    PROPERTYBASELCATEGR       CHAR(2),
    PROPERTYUSE               CHAR(2),
    PROPERTYADDRESS           VARCHAR(50),
    PROPERTYADDRESSNO         VARCHAR(5),
    PROPERTYPOSTALCODE        VARCHAR(10),
    PROPERTYREGION            VARCHAR(30),
    PROPERTYCITY              VARCHAR(30),
    PROPERTYPREFECTURE        VARCHAR(30),
    PROPERTYFLOORNO           VARCHAR(3),
    PROPERTYOWN               CHAR(2),
    PROPERTYOWNPERCENT        SMALLINT,
    PROPERTYCONTRNO           VARCHAR(20),
    PROPERTYBUILDPERMITNO     VARCHAR(20),
    PROPERTYKAEK              VARCHAR(20),
    PROPERTYURBPLANISSUES     VARCHAR(500),
    PROPERTYLEGALISSUES       VARCHAR(500),
    PROPERTYMANIFACTLICENYEAR SMALLINT,
    PROPERTYCONSTRYEAR        SMALLINT,
    PROPERTYTOTLANDAREA       DECIMAL(12, 2),
    PROPERTYMAINUSESIZE       DECIMAL(12, 2),
    PROPERTYPARKWARHSAREA     DECIMAL(12, 2),
    PROPERTYNOTEDAREASIZE     DECIMAL(12, 2),
    PROPERTYOPENSPACLOFTAREA  DECIMAL(12, 2),
    PROPERTYAREAPRICE         DECIMAL(12, 2),
    PROPERTYWARHSNO           SMALLINT,
    PROPERTYPARKNO            SMALLINT,
    PROPERTYWARHSTOTAREA      DECIMAL(12, 2),
    PROPERTYPARKTOTAREA       DECIMAL(12, 2),
    PROPERTYLANDVALUE         DECIMAL(12, 2),
    PROPERTYREALESTVAL        DECIMAL(12, 2),
    PROPERTYWARHSAREAVAL      DECIMAL(12, 2),
    PROPERTYPARKAREAVAL       DECIMAL(12, 2),
    PROPERTYOBJVALUE          DECIMAL(12, 2),
    PROPERTYCONSTRCOST        DECIMAL(12, 2),
    PROPERTYCONTRVAL          DECIMAL(12, 2),
    PROPERTYINSURVAL          DECIMAL(12, 2),
    PROPERTYDISPOSVAL         DECIMAL(12, 2),
    PROPERTYESTIMTR           VARCHAR(50),
    PROPERTYESTIMMETHD        CHAR(1),
    PROPERTYREESTIMFREQ       CHAR(2),
    PROPERTYESTIMLASTDAT      DATE,
    PROPERTYNEXTESTIMDAT      DATE,
    PROPERTYANNRENT           DECIMAL(12, 2),
    PROPERTYDESCRPT           VARCHAR(500),
    PROPERTYISINSURED         CHAR(1),
    PROPERTYINSURAMT          DECIMAL(12, 2),
    PROPERTYINSURCOMPN        VARCHAR(50),
    PROPERTYINSURINFOS        VARCHAR(500),
    PROPERTYOTHNOTES          VARCHAR(500),
    DEPOSITBANK_ID            CHAR(2),
    DEPOSITBANKBRANCH         CHAR(5),
    DEPOSITBANKACC            VARCHAR(17),
    DEPOSITBANKIBAN           CHAR(27),
    SHARECATEGORY             VARCHAR(20),
    SHAREISIN                 VARCHAR(20),
    SHARECOMPNAME             VARCHAR(50),
    SHARECODE                 VARCHAR(20),
    SHAREPUBLISHER            VARCHAR(50),
    SHARETITLENUM             VARCHAR(50),
    SHAREPARVALUE             DECIMAL(12, 2),
    SHAREBOOKVALUE            DECIMAL(12, 2),
    SHAREBOOKVALYEAR          CHAR(4),
    SHARENO                   DECIMAL(10),
    SHAREVALUE                DECIMAL(12, 2),
    PRODUCTQUANTITY           DECIMAL(12, 2),
    PRODUCTMEASURUNIT         CHAR(2),
    PRODUCTUNITVALUE          DECIMAL(12, 2),
    BONDSISSUEDATE            DATE,
    BONDSENDDATE              DATE,
    BONDSTYPE                 VARCHAR(2),
    BONDSPUBLISHER            VARCHAR(50),
    BONDSDEPOSITARY           VARCHAR(50),
    NAVYMORTGTYPE             VARCHAR(2),
    NAVYOWNTITLENO            VARCHAR(20),
    NAVYMORTGAMT              DECIMAL(12, 2),
    NAVYBOATNAME              VARCHAR(50),
    NAVYBOATTYPE              VARCHAR(50),
    NAVYREGNUM                VARCHAR(50),
    ASSIGNSECTOR              VARCHAR(2),
    ASSIGNSECTRNAME           VARCHAR(50),
    ASSIGNSECTRDPT            VARCHAR(50),
    ASSIGNSECTRPHONE          VARCHAR(20),
    ASSIGNSECTRTYPE           VARCHAR(2),
    ASSIGNSECTRDATE           DATE,
    ASSIGNSECTRENDDATE        DATE,
    ASSIGNSECTRPAYOFF         DATE,
    GUARANTORTYPE             VARCHAR(2),
    GUARANTORNAME             VARCHAR(50),
    GUARANTORSECTNAME         VARCHAR(50),
    GUARANTORSECTADDRESS      VARCHAR(50),
    GUARANTEEDECNO            VARCHAR(50),
    GUARANTEEACTNO            VARCHAR(50),
    GUARANTEETEMPEPROGRAM     VARCHAR(50),
    GUARANTEETEMPEGUARANTEENO VARCHAR(50),
    CHEQUEBANKID              CHAR(2),
    CHEQUEBANKACC             VARCHAR(30),
    CHEQUENO                  VARCHAR(20),
    CHEQUEAMOUNT              DECIMAL(12, 2),
    CHEQUECURRENCY            VARCHAR(3),
    CHEQUEENDDATE             DATE,
    CHEQUEISSUERNAME          VARCHAR(50),
    CHEQUEISSUERAFM           VARCHAR(10),
    CHEQUEISSUERCUSTMRID      VARCHAR(15),
    CHEQUESTATUS              SMALLINT,
    CHEQUESTAMPDATE           DATE,
    DATADATE                  DATE        not null,
    EXPORTDATE                DATE        not null,
    TMSTAMP_COLLATERAL        TIMESTAMP(6),
    COLLAT_TABLE_KEY          VARCHAR(12),
    COLLAT_DTL_KEY            VARCHAR(25),
    constraint PK_CLC_INT_ASSETS
        primary key (ASSET_ID, EXPORTDATE)
);

comment on table CLC_INT_ASSETS is 'This table contains analytic data about assets categorized per asset type';

comment on column CLC_INT_ASSETS.ASSET_ID is 'Asset Unique ID -';

comment on column CLC_INT_ASSETS.TYPE is 'Asset Type';

comment on column CLC_INT_ASSETS.SHORTDESCR is 'Short Description -';

comment on column CLC_INT_ASSETS.CIF is 'Asset Owner (CIF) -';

comment on column CLC_INT_ASSETS.ASSETVALUE is 'Asset Value ( ) - PROPERTY: REAL_ESTATE.COMMERCIAL_VAL_AMN';

comment on column CLC_INT_ASSETS.PROPERTYCATEGORY is 'Property Category/ . HFC (  ) - REAL_ESTATE.FK_GD_CAT_BANK';

comment on column CLC_INT_ASSETS.PROPERTYTYPE is 'Property Type/  ( ) - REAL_ESTATE.FK_GD_CAT';

comment on column CLC_INT_ASSETS.PROPERTYBASELCATEGR is 'Basel Property Category/     - REAL_ESTATE. FK_GD_BASAK';

comment on column CLC_INT_ASSETS.PROPERTYUSE is 'Property Use/  - REAL_ESTATE. TITLE_NATURE_TYPE (1= , 2= , 3= Mailo Land)';

comment on column CLC_INT_ASSETS.PROPERTYADDRESS is 'Property Address/ - REAL_ESTATE. ADDRESS';

comment on column CLC_INT_ASSETS.PROPERTYADDRESSNO is 'Property Address Number/ - REAL_ESTATE.ADDRESS_NUM';

comment on column CLC_INT_ASSETS.PROPERTYPOSTALCODE is 'Property Postal Code/.. - REAL_ESTATE.ZIP_CODE';

comment on column CLC_INT_ASSETS.PROPERTYREGION is 'Property Region/ - REAL_ESTATE.REGION';

comment on column CLC_INT_ASSETS.PROPERTYCITY is 'Property City/// - REAL_ESTATE.CITY';

comment on column CLC_INT_ASSETS.PROPERTYPREFECTURE is 'Property Prefecture/ - REAL_ESTATE.FK_GD_ADDDI';

comment on column CLC_INT_ASSETS.PROPERTYFLOORNO is 'Property Floor No/.  - REAL_ESTATE.FK_GD_FLOOR';

comment on column CLC_INT_ASSETS.PROPERTYOWN is 'Property Ownership/  - REAL_ESTATE_CUST.FK_GD_OWNER_TYPE';

comment on column CLC_INT_ASSETS.PROPERTYOWNPERCENT is 'Property Ownership Percent/  (%) - REAL_ESTATE_CUST.REAL_ESTATE_CUST';

comment on column CLC_INT_ASSETS.PROPERTYCONTRNO is 'Property Contract Number/.  -';

comment on column CLC_INT_ASSETS.PROPERTYBUILDPERMITNO is 'Property Building Permit No/.   - REAL_ESTATE.BUILD_LICENCEID';

comment on column CLC_INT_ASSETS.PROPERTYKAEK is 'Property /.... - REAL_ESTATE.LAND_REGIST_ID';

comment on column CLC_INT_ASSETS.PROPERTYURBPLANISSUES is 'Property Urban Planning Issues/  - REAL_ESTATE.URBAN_PLAN_PRO';

comment on column CLC_INT_ASSETS.PROPERTYLEGALISSUES is 'Property Legal Issues/  - REAL_ESTATE.LEGAL_PROBLEMS';

comment on column CLC_INT_ASSETS.PROPERTYMANIFACTLICENYEAR is 'Property Manufacturing License Year/      - REAL_ESTATE.ISSUE_YEAR';

comment on column CLC_INT_ASSETS.PROPERTYCONSTRYEAR is 'Property Construction Year/  - REAL_ESTATE.CONSTRUCTION_YEAR';

comment on column CLC_INT_ASSETS.PROPERTYTOTLANDAREA is 'Property Total Land Area/   - REAL_ESTATE.TOTAL_SITE_AREA';

comment on column CLC_INT_ASSETS.PROPERTYMAINUSESIZE is 'Property Main Use Areas Size/    - REAL_ESTATE.MAIN_AREA';

comment on column CLC_INT_ASSETS.PROPERTYPARKWARHSAREA is 'Property Parking Warehouse Area/   /  - REAL_ESTATE_INFO.FK_GD_HAS_ADD_INFO';

comment on column CLC_INT_ASSETS.PROPERTYNOTEDAREASIZE is 'Property Noted Areas Size/   - REAL_ESTATE.MORTGAGE_AREA';

comment on column CLC_INT_ASSETS.PROPERTYOPENSPACLOFTAREA is 'Property Open Spaces Loft Area/     - REAL_ESTATE.OUTDOOR_ATTIC_A';

comment on column CLC_INT_ASSETS.PROPERTYAREAPRICE is 'Property Area Price/  - REAL_ESTATE_APPRSL.PRICE_PER_METER';

comment on column CLC_INT_ASSETS.PROPERTYWARHSNO is 'Property Warehouse No/.  - REAL_ESTATE.STORE_ROOM_NUM';

comment on column CLC_INT_ASSETS.PROPERTYPARKNO is 'Property Parkings No/.   - REAL_ESTATE.TOT_PARK_NUM';

comment on column CLC_INT_ASSETS.PROPERTYWARHSTOTAREA is 'Property Warehouses Total Area/   - REAL_ESTATE.TOT_STORE_AREA';

comment on column CLC_INT_ASSETS.PROPERTYPARKTOTAREA is 'Property Parkings Total Area/    - REAL_ESTATE.TOT_PARK_AREA';

comment on column CLC_INT_ASSETS.PROPERTYLANDVALUE is 'Property Land Value/ O - REAL_ESTATE.SITE_VALUE_AMN';

comment on column CLC_INT_ASSETS.PROPERTYREALESTVAL is 'Property Real Estate Value/ A - REAL_ESTATE.REAL_EST_VAL_AMN';

comment on column CLC_INT_ASSETS.PROPERTYWARHSAREAVAL is 'Property Warehouses Area Value/ A - REAL_ESTATE.STORE_VALUE_AMN';

comment on column CLC_INT_ASSETS.PROPERTYPARKAREAVAL is 'Property Parkings Area Value/   - REAL_ESTATE.PARKING_VAL_AMN';

comment on column CLC_INT_ASSETS.PROPERTYOBJVALUE is 'Property Objective Value/  - REAL_ESTATE.OBJECTIVE_AMN ORREAL_ESTATE_APPRSL.OBJECTIVE_AMN';

comment on column CLC_INT_ASSETS.PROPERTYCONSTRCOST is 'Property Construction Cost/  - REAL_ESTATE.CONSTR_COST_AMN';

comment on column CLC_INT_ASSETS.PROPERTYCONTRVAL is 'Property Contract Value/  - REAL_ESTATE.AGREEMENT_VALUE';

comment on column CLC_INT_ASSETS.PROPERTYINSURVAL is 'Property Insurance Value/  - REAL_ESTATE.INS_ORIGINAL_AMN';

comment on column CLC_INT_ASSETS.PROPERTYDISPOSVAL is 'Property Disposal Value/   (  ) - REAL_ESTATE_APPRSL.SELL_VALUE';

comment on column CLC_INT_ASSETS.PROPERTYESTIMTR is 'Property Estimator/ - REAL_ESTATE_APPRSL.EVALUATOR';

comment on column CLC_INT_ASSETS.PROPERTYESTIMMETHD is 'Property Estimation Method/  - REAL_ESTATE_APPRSL.FK_GD_VAL_TYPE';

comment on column CLC_INT_ASSETS.PROPERTYREESTIMFREQ is 'Property Re Estimation Frequency/  - REAL_ESTATE.REEVAL_FREQ';

comment on column CLC_INT_ASSETS.PROPERTYESTIMLASTDAT is 'Property Estimation Last Date/.   - REAL_ESTATE_APPRSL.EVALUATION_DT';

comment on column CLC_INT_ASSETS.PROPERTYNEXTESTIMDAT is 'Property Next Estimation Date/.   - REAL_ESTATE_APPRSL.EVALUATION_DT + REEVAL_FREQ = 1 (+  6months)REEVAL_FREQ = 2 (+ 12months)REEVAL_FREQ = 3 (+ 36months)REEVAL_FREQ = 4 (+ 48months)';

comment on column CLC_INT_ASSETS.PROPERTYANNRENT is 'Property Annual Rent/  - REAL_ESTATE.ANNUAL_RENT_AMN';

comment on column CLC_INT_ASSETS.PROPERTYDESCRPT is 'Property Description/ - REAL_ESTATE.REAL_ESTATE_DESC';

comment on column CLC_INT_ASSETS.PROPERTYISINSURED is 'Property Is Insured/    -';

comment on column CLC_INT_ASSETS.PROPERTYINSURAMT is 'Property Insurance Amount/  - REAL_ESTATE.INSURANCE_AMN';

comment on column CLC_INT_ASSETS.PROPERTYINSURCOMPN is 'Property Insurance Company/  - REAL_ESTATE.FK_GD_INS_COMP';

comment on column CLC_INT_ASSETS.PROPERTYINSURINFOS is 'Property Insurance Infos/   - REAL_ESTATE.INSURANCE_DESCR';

comment on column CLC_INT_ASSETS.PROPERTYOTHNOTES is 'Property Other Notes/      - REAL_ESTATE.ADD_DESCR1 +REAL_ESTATE.ADD_DESCR2 +REAL_ESTATE.ADD_DESCR3 +REAL_ESTATE.ADD_DESCR4 +REAL_ESTATE.ADD_DESCR5';

comment on column CLC_INT_ASSETS.DEPOSITBANK_ID is 'Deposit Bank ID/ LOV -';

comment on column CLC_INT_ASSETS.DEPOSITBANKBRANCH is 'Deposit Bank Branch/ -';

comment on column CLC_INT_ASSETS.DEPOSITBANKACC is 'Deposit Bank Account/ -';

comment on column CLC_INT_ASSETS.DEPOSITBANKIBAN is 'Deposit Bank IBAN/IBAN -';

comment on column CLC_INT_ASSETS.SHARECATEGORY is 'Share Category/  LOV - COLLATERAL_TABLE.FLAG_1';

comment on column CLC_INT_ASSETS.SHAREISIN is 'Share ISIN - COLLATERAL_TABLE.DESCR_4';

comment on column CLC_INT_ASSETS.SHARECOMPNAME is 'Share Company Name/  -';

comment on column CLC_INT_ASSETS.SHARECODE is 'Share Code/  -';

comment on column CLC_INT_ASSETS.SHAREPUBLISHER is 'Share Publisher/ - COLLATERAL_TABLE .CUST_ID_1';

comment on column CLC_INT_ASSETS.SHARETITLENUM is 'Share Titles Number/  - COLLATERAL_TABLE.DESCR_5';

comment on column CLC_INT_ASSETS.SHAREPARVALUE is 'Share Par Value/   - COLLATERAL_TABLE.AMOUNT_2 ( ) , COLLATERAL_TABLE.AMOUNT_6 ()';

comment on column CLC_INT_ASSETS.SHAREBOOKVALUE is 'Share Book Value/   - COLLATERAL_TABLE.AMOUNT_3 ( )';

comment on column CLC_INT_ASSETS.SHAREBOOKVALYEAR is 'Share Book Value Year/   - COLLATERAL_TABLE.DATE_2 ( )';

comment on column CLC_INT_ASSETS.SHARENO is 'Share No/  - COLLATERAL_TABLE.NUMBER_2';

comment on column CLC_INT_ASSETS.SHAREVALUE is 'Share Value/  - COLLATERAL_TABLE.AMOUNT_7';

comment on column CLC_INT_ASSETS.PRODUCTQUANTITY is 'Product Quantity/ -';

comment on column CLC_INT_ASSETS.PRODUCTMEASURUNIT is 'Product Measurement Unit/LOV -';

comment on column CLC_INT_ASSETS.PRODUCTUNITVALUE is 'Product Unit Value/  -';

comment on column CLC_INT_ASSETS.BONDSISSUEDATE is 'Bonds Issue Date/   - COLLATERAL_TABLE.DATE_2';

comment on column CLC_INT_ASSETS.BONDSENDDATE is 'Bonds End Date/   - COLLATERAL_TABLE.DATE_3';

comment on column CLC_INT_ASSETS.BONDSTYPE is 'Bonds Type/  LOV - COLLATERAL_TABLE.FLAG_1 (1=, 2=)';

comment on column CLC_INT_ASSETS.BONDSPUBLISHER is 'Bonds Publisher/  - COLLATERAL_TABLE.CUST_ID_2';

comment on column CLC_INT_ASSETS.BONDSDEPOSITARY is 'Bonds Depositary/  - COLLATERAL_TABLE.GD_SERIAL_NUM_1';

comment on column CLC_INT_ASSETS.NAVYMORTGTYPE is 'Navy Mortgage Type/  LOV -';

comment on column CLC_INT_ASSETS.NAVYOWNTITLENO is 'Navy Ownership Title No/   -';

comment on column CLC_INT_ASSETS.NAVYMORTGAMT is 'Navy Mortgage Amount/  -';

comment on column CLC_INT_ASSETS.NAVYBOATNAME is 'Navy Boat Name/  -';

comment on column CLC_INT_ASSETS.NAVYBOATTYPE is 'Navy Boat Type/ (, , ) -';

comment on column CLC_INT_ASSETS.NAVYREGNUM is 'Navy Register Number/.  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTOR is 'Assignment Sector Type/LOV -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRNAME is 'Assignment Sector Name/  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRDPT is 'Assignment Sector Department//  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRPHONE is 'Assignment Sector Phone/  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRTYPE is 'Assignment Type/ LOV -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRDATE is 'Assignment Date/  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRENDDATE is 'Assignment End Date/  -';

comment on column CLC_INT_ASSETS.ASSIGNSECTRPAYOFF is 'Assignment Payoff Date/ ';

comment on column CLC_INT_ASSETS.GUARANTORTYPE is 'Guarantor Type/  LOV -';

comment on column CLC_INT_ASSETS.GUARANTORNAME is 'Guarantor Name/  -';

comment on column CLC_INT_ASSETS.GUARANTORSECTNAME is 'Guarantor Sector Name/  -';

comment on column CLC_INT_ASSETS.GUARANTORSECTADDRESS is 'Guarantor Sector Address//  -';

comment on column CLC_INT_ASSETS.GUARANTEEDECNO is 'Guarantee Decision No/.  -';

comment on column CLC_INT_ASSETS.GUARANTEEACTNO is 'Guarantee Act No/.   -';

comment on column CLC_INT_ASSETS.GUARANTEETEMPEPROGRAM is 'Guarantee TEMPE Program/ () -';

comment on column CLC_INT_ASSETS.GUARANTEETEMPEGUARANTEENO is 'Guarantee TEMPE Guarantee No/.  () -';

comment on column CLC_INT_ASSETS.CHEQUEBANKID is 'Cheque Bank ID/BANKID -';

comment on column CLC_INT_ASSETS.CHEQUEBANKACC is 'Cheque Bank Account/  IBAN -';

comment on column CLC_INT_ASSETS.CHEQUENO is 'Cheque No/.  -';

comment on column CLC_INT_ASSETS.CHEQUEAMOUNT is 'Cheque Amount/  -';

comment on column CLC_INT_ASSETS.CHEQUECURRENCY is 'Cheque Currency/ -';

comment on column CLC_INT_ASSETS.CHEQUEENDDATE is 'Cheque End Date/  -';

comment on column CLC_INT_ASSETS.CHEQUEISSUERNAME is 'Cheque Issuer Name/  -';

comment on column CLC_INT_ASSETS.CHEQUEISSUERAFM is 'Cheque Issuer AFM/  -';

comment on column CLC_INT_ASSETS.CHEQUEISSUERCUSTMRID is 'Cheque Issuer Customer ID/   -';

comment on column CLC_INT_ASSETS.CHEQUESTATUS is 'Cheque Status/Status  LOV -';

comment on column CLC_INT_ASSETS.CHEQUESTAMPDATE is 'Cheque Stamp Date/  -';

comment on column CLC_INT_ASSETS.DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ASSETS.EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

