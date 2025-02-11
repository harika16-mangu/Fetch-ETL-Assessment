--=============================Creating tables within Analytics database and public schema========================
--================================================================================================================
--*****************Creating tables to pipeline the tables from airflow to snowflake********************
--***1.Database- Analytics***
--***2.Schema- Public***
--***3.Tables- Users,Brands,Receipts,Receipt_Items,Products,Rewards,Userflagged,Metabrite***

GRANT USAGE ON DATABASE ANALYTICS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA ANALYTICS.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ANALYTICS.PUBLIC TO ROLE ACCOUNTADMIN;

SHOW DATABASES;
SHOW SCHEMAS IN ANALYTICS;
SHOW TABLES IN ANALYTICS.PUBLIC;

--creation of table users
CREATE TABLE ANALYTICS.PUBLIC.USERS (
    user_id STRING PRIMARY KEY,
    active BOOLEAN,
    role STRING,
    signUpSource STRING,
    state STRING,
    createdDate TIMESTAMP_NTZ,
    lastLogin TIMESTAMP_NTZ
);

--creation of table brands
CREATE TABLE ANALYTICS.PUBLIC.BRANDS (
    brand_id STRING PRIMARY KEY,
    barcode STRING,
    brandCode STRING,
    category STRING,
    categoryCode STRING,
    cpg_id STRING,
    name STRING,
    topBrand STRING
);

--creation of table receipts
CREATE TABLE ANALYTICS.PUBLIC.RECEIPTS (
    receipt_id string PRIMARY KEY,
    userId string,
    bonusPointsEarned integer,
    pointsEarned integer,
    purchasedItemCount integer,
    totalSpent integer,
    rewardsReceiptStatus string,
    createDate timestamp_ntz,
    dateScanned timestamp_ntz,
    finishedDate timestamp_ntz,
    modifyDate timestamp_ntz,
    pointsAwardedDate timestamp_ntz,
    purchaseDate timestamp_ntz
);

--creation of table products
create table ANALYTICS.PUBLIC.PRODUCTS(
    barcode string,
    description string,
    itemPrice float,
    targetPrice float,
    discountedItemPrice float,
    finalPrice float,
    competitiveProduct boolean,
    needsFetchReview boolean,
    needsFetchReviewReason string,
    product_id integer PRIMARY KEY
);

--creation of table rewards
create table ANALYTICS.PUBLIC.REWARDS(
    rewardsProductPartnerId string,
    rewardsGroup string,
    pointsEarned integer,
    pointsPayerId string,
    pointsNotAwardedReason string,
    reward_id integer PRIMARY KEY
);

--creation of userflagged
create table ANALYTICS.PUBLIC.USER_FLAGGED_ITEMS(
    userFlaggedBarcode string,
    userFlaggedDescription string,
    userFlaggedPrice float,
    userFlaggedQuantity integer,
    userFlaggedNewItem boolean,
    user_flagged_id integer PRIMARY KEY
);

--creation of table receipts_items
create table ANALYTICS.PUBLIC.RECEIPT_ITEMS(
    receipt_id string PRIMARY KEY,
    barcode string,
    partnerItemId integer,
    preventTargetGapPoints boolean,
    quantityPurchased integer,
    userFlaggedBarcode string,
    originalReceiptItemText string,
    originalFinalPrice float,
    originalMetaBriteBarcode string,
    rewardsProductPartnerId string
);

--creation of table metabrite_items
create table ANALYTICS.PUBLIC.METABRITE_ITEMS(
    originalMetaBriteBarcode string,
    originalMetaBriteDescription string,
    originalMetaBriteItemPrice float,
    originalMetaBriteQuantityPurchased integer,
    metabrite_id integer primary key
