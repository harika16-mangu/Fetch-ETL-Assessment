--==================================== Data Type Validations ==================================================
--=============================================================================================================
/*DATA TYPE VALIDATION IS DONE TO ENSURE THE DATATYPES OF EACH COLUMNS TO AVOID DATA INTEGRITY ISSUES (e.g., numeric, date, text)
NOTE: DATA TYPE VALIDATION IS DONE DURING PIPELINING SO WRITING SQL QUERIES TO VERIFY SO THAT WE CAN UPDATE IF NECESSARY
1.Prevention of processing errors:Ensuring data entries have expected datatypes
2.Enhancing Data Reliabilty across platforms: Enforcing correct data types to maintain consistency*/

--****************** 1.Data Validation for Users table*******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'USERS'

--*************** 2. Data Validation for Brand Table*******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'BRANDS'
--******************3. Data Validations for Receipts Table******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'RECEIPTS'
--******************4. Data Validations for Receipts_Item Table******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'RECEIPT_ITEMS'
--******************5. Data Validations for Products Table******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'PRODUCTS'
--******************6. Data Validations for Rewards Table*****************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'REWARDS'
--*****************7. Data Validations for Userflagged_Items Table****************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'USER_FLAG_ITEMS'
--*****************8. Data Validations for Metabrite_Items Table******************
SELECT
    COLUMN_NAME AS "Column Name",
    DATA_TYPE AS "Data Type"
FROM
    ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = 'PUBLIC'
    AND TABLE_NAME = 'METABRITE_ITEMS'
