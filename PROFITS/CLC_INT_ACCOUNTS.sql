create table CLC_INT_ACCOUNTS
(
    ACC_ID                        VARCHAR(17)    not null,
    BP_ID                         VARCHAR(20)    not null,
    ACC_PRODUCTCODE               VARCHAR(50)    not null,
    ACC_SUBPRODUCTCODE            VARCHAR(6),
    ACC_DELAYSTATUS               VARCHAR(50)    not null,
    ACC_NPLFLAG                   CHAR(2),
    ACC_NPLFLAGLASTUPD            DATE,
    ACC_FRBFLAG                   CHAR(2),
    ACC_FRBFLAGLASTUPD            DATE,
    ACC_MODIFIEDDATE              DATE,
    ACC_MODIFIEDTYPE              VARCHAR(32),
    ACC_REGULATIONINFO            VARCHAR(50),
    ACC_REGULATIONTYPEDETAIL      VARCHAR(50),
    ACC_NEWREGULATIONFLAG         CHAR(50),
    ACC_FINALIZEDFLAG             CHAR(50),
    ACC_BUSINESSUNIT              CHAR(20),
    ACC_CONTRACTNUM               VARCHAR(17),
    ACC_OPENDATE                  DATE,
    ACC_EXPDATE                   DATE,
    ACC_BILLINGDATE               DATE,
    ACC_TOTALDEBPAMT              DECIMAL(18, 3),
    ACC_DUEDATE                   DATE,
    ACC_BUCKETNO                  SMALLINT       not null,
    ACC_DELDAYS                   INTEGER        not null,
    ACC_OLDESTOVERDUEDT           DATE,
    ACC_CURRCODE                  CHAR(3)        not null,
    ACC_CHARGEOFFDT               DATE,
    ACC_LASTPMAMT                 DECIMAL(18, 3),
    ACC_LASTPMT_DATE              DATE,
    ACC_CREDLMT                   DECIMAL(18, 3),
    ACC_MINPAYAMT                 DECIMAL(18, 3),
    ACC_OSBALANCE                 DECIMAL(18, 3) not null,
    ACC_DELINQAMT                 DECIMAL(18, 3) not null,
    ACC_FROZENAMT                 DECIMAL(18, 3) not null,
    ACC_CAPITALAMT                DECIMAL(18, 3) not null,
    ACC_OVRDINTREQ                DECIMAL(18, 3) not null,
    ACC_OVRDINTAMT                DECIMAL(18, 3) not null,
    ACC_OTHERAMT                  DECIMAL(18, 3) not null,
    ACC_OVLMTAMT                  DECIMAL(18, 3) not null,
    ACC_DISBURSEDAMT              DECIMAL(18, 3),
    ACC_DURATION                  SMALLINT,
    ACC_REMINSTALL                SMALLINT,
    ACC_IBANNUM                   CHAR(26),
    ACC_INTRATE                   DECIMAL(18, 4),
    ACC_PAYOFFCAPITALAMT          DECIMAL(18, 3),
    ACC_PAYOFFINTERESTAMT         DECIMAL(18, 3),
    ACC_PAYOFFTOTALAMT            DECIMAL(18, 3),
    ACC_WRITEOFFAMT               CHAR(26),
    ACC_ACCRUALSAMT               DECIMAL(18, 3),
    ACC_WRITEOFFDATE              DATE,
    ACC_NEXTBILLINGDATE           DATE,
    ACC_DATADATE                  DATE           not null,
    ACC_EXPORTDATE                DATE           not null,
    TMSTAMP_PRODUCT               TIMESTAMP(6),
    TMSTAMP_LOAN_ACCOUNT          TIMESTAMP(6),
    TMSTAMP_LG_ACCOUNT            TIMESTAMP(6),
    OLD_ACC_ID                    VARCHAR(40),
    ACC_NON_PERF_EXPOSURE         CHAR(1),
    ACC_FORBORNE_FLAG             CHAR(1),
    ACC_FORBEARANCE_ENDDATE       DATE,
    ACC_NON_PERFORMING_ENDDATE    DATE,
    ACC_TOTAL_DELINQ_INSTALLMENTS INTEGER,
    ACC_BAD_DEBTS                 DECIMAL(18, 3),
    ACC_ADJUSTMENT_DATE           DATE,
    constraint PK_CLC_INT_ACCOUNT
        primary key (ACC_ID, ACC_EXPORTDATE)
);

comment on table CLC_INT_ACCOUNTS is 'This table will contain records, each one representing an account for (collections) treatment. If possible all product types (e.g. loans, mortgages, credit cards, car loans, etc.) can be exported in the same layout. One record should be exported for each';

comment on column CLC_INT_ACCOUNTS.ACC_ID is 'Unique Account Number - PROFITS_ACCOUNT.ACCOUNT_NUMBER';

comment on column CLC_INT_ACCOUNTS.BP_ID is 'Customer ID of Prime Owner - PROFITS_ACCOUNT.CUST_ID';

comment on column CLC_INT_ACCOUNTS.ACC_PRODUCTCODE is 'Account Product Code/  AroTRON:- Business- Retail Loans- Mortgage Loans- Credit cards- LoG (Letters of Guarantees)- Stamped Checks/ ';

comment on column CLC_INT_ACCOUNTS.ACC_SUBPRODUCTCODE is 'Sub-Product Code -';

comment on column CLC_INT_ACCOUNTS.ACC_DELAYSTATUS is 'Delay Status/Status - Current/ - Temporary Delay/ - Permanent Delay/ - Fearfulness/, Write off/- Closed/==================================LOAN_ACCOUNT.LOAN_STATUS 1=';

comment on column CLC_INT_ACCOUNTS.ACC_NPLFLAG is 'NPL Flag (8  + 1   no flag';

comment on column CLC_INT_ACCOUNTS.ACC_NPLFLAGLASTUPD is 'NPL Flag Last Update/   NPL Flag -';

comment on column CLC_INT_ACCOUNTS.ACC_FRBFLAG is 'FRB Flag (current status)- Bankruptcy- Pre-Bankruptcy- Setup/ - Modification/- Setup/ - Refunding/- NPLFlag is de-activated:- Settings/ -> Flag Inactive Setup/ - < all scenarios should be';

comment on column CLC_INT_ACCOUNTS.ACC_FRBFLAGLASTUPD is 'FRB Flag Last Update/   FRB Flag -';

comment on column CLC_INT_ACCOUNTS.ACC_MODIFIEDDATE is 'Modification Date -';

comment on column CLC_INT_ACCOUNTS.ACC_MODIFIEDTYPE is 'ACC_ModifiedType -';

comment on column CLC_INT_ACCOUNTS.ACC_REGULATIONINFO is 'Regulation Information/ Setup Duration/ :- Long-term/- Short-term/- Final Settlement Solution/  ';

comment on column CLC_INT_ACCOUNTS.ACC_REGULATIONTYPEDETAIL is 'Regulation Type/ : ... (        ) -';

comment on column CLC_INT_ACCOUNTS.ACC_NEWREGULATIONFLAG is 'New Regulation (yes or no)// (  ) -';

comment on column CLC_INT_ACCOUNTS.ACC_FINALIZEDFLAG is 'Finalization (yes or no)/ (  ) -';

comment on column CLC_INT_ACCOUNTS.ACC_BUSINESSUNIT is 'Business Unit (   NPL Flag) -';

comment on column CLC_INT_ACCOUNTS.ACC_CONTRACTNUM is 'Contract Number -';

comment on column CLC_INT_ACCOUNTS.ACC_OPENDATE is 'Account Open Date - LOAN_ACCOUNT. ACC_OPEN_DT';

comment on column CLC_INT_ACCOUNTS.ACC_EXPDATE is 'Last installment due date (          ) - LOAN_ACCOUNT. ACC_EXP_DT';

comment on column CLC_INT_ACCOUNTS.ACC_BILLINGDATE is 'Next billing date (: .       ,  ) / :            ===============================';

comment on column CLC_INT_ACCOUNTS.ACC_TOTALDEBPAMT is 'Next Debt Amount ( / :    )orTotal Debt Amount (:      30 )';

comment on column CLC_INT_ACCOUNTS.ACC_DUEDATE is 'Due date following the last bill date. -';

comment on column CLC_INT_ACCOUNTS.ACC_BUCKETNO is 'Bucket Number(Number of installments delinquent)     :- Days of Delay/  / 30- 0-6- 6-12 a 12- 12-24 -> 24- 24-36 -> 36- 36-48 -> 48- 48-60 -> 60- > 5  -> 99';

comment on column CLC_INT_ACCOUNTS.ACC_DELDAYS is 'Delinquent Days/  - LOAN_ACCOUNT.TOLERANCE_DAYS';

comment on column CLC_INT_ACCOUNTS.ACC_OLDESTOVERDUEDT is 'Oldest Overdue Date/   -';

comment on column CLC_INT_ACCOUNTS.ACC_CURRCODE is 'Currency (ISO Standard) - LOAN_ACCOUNT. FKCUR_IS_MOVED_IN';

comment on column CLC_INT_ACCOUNTS.ACC_CHARGEOFFDT is 'Charge Off Date/   -';

comment on column CLC_INT_ACCOUNTS.ACC_LASTPMAMT is 'Last Payment Amount            AroTRON';

comment on column CLC_INT_ACCOUNTS.ACC_LASTPMT_DATE is 'Last payment date (transaction date)            AroTRON';

comment on column CLC_INT_ACCOUNTS.ACC_CREDLMT is 'Credit Limit /   -';

comment on column CLC_INT_ACCOUNTS.ACC_MINPAYAMT is 'Minimum payment amount -';

comment on column CLC_INT_ACCOUNTS.ACC_OSBALANCE is 'Outstanding balance: Sum of all billed (both due and not due) unpaid installments.       ( .. )';

comment on column CLC_INT_ACCOUNTS.ACC_DELINQAMT is 'Delinquent amount: Sum of all billed and due (unpaid) installments.  ( and    )';

comment on column CLC_INT_ACCOUNTS.ACC_FROZENAMT is '    -';

comment on column CLC_INT_ACCOUNTS.ACC_CAPITALAMT is 'Capital (Principal that is unpaid)   -';

comment on column CLC_INT_ACCOUNTS.ACC_OVRDINTREQ is 'Overdue Interest Requirements    () -';

comment on column CLC_INT_ACCOUNTS.ACC_OVRDINTAMT is 'Overdue Interests (Late Payment Fee + Overdue interest up to day) -';

comment on column CLC_INT_ACCOUNTS.ACC_OTHERAMT is 'Other expenses and fees (e.g. monthly Statement Fee, Collections/Legal fees, insurance, other fees etc.) -';

comment on column CLC_INT_ACCOUNTS.ACC_OVLMTAMT is 'Over limit amount   -';

comment on column CLC_INT_ACCOUNTS.ACC_DISBURSEDAMT is 'Total disbursed amount   ( / ) -';

comment on column CLC_INT_ACCOUNTS.ACC_DURATION is 'Loan Tenor in Months - LOAN_ACCOUNT.INSTALL_COUNT';

comment on column CLC_INT_ACCOUNTS.ACC_REMINSTALL is 'Remaining Installments (: , / :    ) - LOAN_ACCOUNT.INSTALL_COUNT - LOAN_ACCOUNT.REQ_INSTALL_SN';

comment on column CLC_INT_ACCOUNTS.ACC_IBANNUM is 'Clearing account number -';

comment on column CLC_INT_ACCOUNTS.ACC_INTRATE is 'Current Interest Rate (Final Rate) - GETLOAN_RATE(@profits_account)';

comment on column CLC_INT_ACCOUNTS.ACC_PAYOFFCAPITALAMT is 'Pay Off Capital Amount (Balance)/  () - LOAN_ACCOUNT.NRM_CAP_BAL';

comment on column CLC_INT_ACCOUNTS.ACC_PAYOFFINTERESTAMT is 'Pay Off Interest Amount /  ( +  ) -';

comment on column CLC_INT_ACCOUNTS.ACC_PAYOFFTOTALAMT is 'Pay Off Total Amount (Total Balance)/  -';

comment on column CLC_INT_ACCOUNTS.ACC_WRITEOFFAMT is 'Write-Off Amount -';

comment on column CLC_INT_ACCOUNTS.ACC_ACCRUALSAMT is 'Accruals - LOAN_ACCOUNT_INFO.NRM_ACCRUAL_AMN + LOAN_ACCOUNT_INFO.OV_NRM_ACCRUAL_AMN';

comment on column CLC_INT_ACCOUNTS.ACC_WRITEOFFDATE is 'Write-Off Date -';

comment on column CLC_INT_ACCOUNTS.ACC_NEXTBILLINGDATE is '  ::    .  .     / :  .   =======================================IF LOAN_ACCOUNT.DRAWDOWN_EXPDT > CURRENTDT THEN DRAWDOWN_EXPDTELSE LO';

comment on column CLC_INT_ACCOUNTS.ACC_DATADATE is 'Date that data refer to - FILLED BY EXPORT APP';

comment on column CLC_INT_ACCOUNTS.ACC_EXPORTDATE is 'Export date - FILLED BY EXPORT APP';

