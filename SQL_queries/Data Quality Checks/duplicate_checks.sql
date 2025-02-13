--================================= Identifying Duplicate records =========================================
--=========================================================================================================
/*Checking for duplicate records in all tables helps us to ensure data quality and reliabilty
1.Data Accuracy and Integrity:Duplicates lead to overrepresentations of entities and skeweness in analysis
2.Cost Saving: Storing duplicates increases storage costs,hence deduplication optimize database performance
3.Improved Analysis: Removing duplicates will improve precision in our analysis and leads to better decision makings*/

--******************1. Duplicates in Users Table******************
/*Duplicates are identified based on composite keys for each table*/
--Count of duplicate user_ids created on same date and login on same date
SELECT USER_ID, COUNT(*)
FROM analytics.public.users
GROUP BY USER_ID,lastlogin
HAVING COUNT(*) > 1;
--Identifying all records by keeping one record per each list of duplicate users
WITH duplicates AS (
  SELECT
    USER_ID,
    CREATEDDATE,
    ROW_NUMBER() OVER (PARTITION BY USER_ID, CREATEDDATE, LASTLOGIN ORDER BY USER_ID) AS rn
  FROM analytics.public.USERS
)
SELECT * FROM duplicates WHERE rn > 1

--******************2.Duplicates in Brands Table******************
--Count of duplicate brand ID's from brands table
SELECT brand_id, COUNT(*)
FROM brands_backup
GROUP BY brand_id
HAVING COUNT(*) > 1;
--Identifying all records by keeping one record per each list of duplicate brands
WITH duplicates AS (
  SELECT
    BRAND_ID,
    NAME,
    ROW_NUMBER() OVER (PARTITION BY BRAND_ID,NAME ORDER BY BRAND_ID) AS rn
  FROM analytics.public.BRANDS
)
SELECT * FROM duplicates WHERE rn > 1

--******************* 3.Duplicates in Receipts Table******************
--checking for duplicates where receipts were created and scanned on same date and the points earned are same
SELECT receipt_id,createdate, datescanned, pointsearned, COUNT(*) AS group_size
FROM analytics.public.receipts
GROUP BY receipt_id, datescanned, pointsearned,createdate
HAVING COUNT(*) > 1
ORDER BY group_size DESC
--Identifying all records by keeping one record per each list of duplicate brands
WITH Duplicates as (
select receipt_id,
	createdate,
	datescanned,
	row_number() over(partition by receipt_id,createdate,datescanned,pointsearned order by createdate desc) as ranks from receipts
)
select * from Duplicates where ranks>1;
--After identifying we can delete the rows giving the delete command 

--*******************4.Duplicates in Recipts Items Table*******************
--Identifying records having the same receipt id, barcode,patneritemid which may occur as its many to one relationship but conidering quantitypurchased to make sure its duplicate.
WITH duplicates AS (
  SELECT
    RECEIPT_ID,BARCODE,PATNERITEMID,QUANTITYPURCHASED
    ROW_NUMBER() OVER (PARTITION BY RECEIPT_ID,BARCODE,PATNERITEMID,QUANTITYPURCHASED ORDER BY RECEIPT_ID) AS rn
  FROM analytics.public.RECEIPT_ITEMS
)
SELECT * FROM duplicates WHERE rn > 1

--*******************As the tables products,rewards,userflagged,metabrite are derived from recipts items this forms many to many relationship and hence handling the duplicates wrt to created primary of each table********************
--******************5. Duplicates in Products Table******************
--Identifying duplicates with same product_id
WITH duplicates AS (
  SELECT
    RECEIPT_ID,
    ROW_NUMBER() OVER (PARTITION BY RECEIPT_ID ORDER BY RECEIPT_ID) AS rn
  FROM analytics.public.RECEIPT_ITEMS
)
SELECT * FROM duplicates WHERE rn > 1

--*****************6.Duplicates in Rewards Table*********************
WITH duplicates AS (
  SELECT
    reward_id,
    ROW_NUMBER() OVER (PARTITION BY reward_id ORDER BY reward_id) AS rn
  FROM analytics.public.rewards
)
SELECT * FROM duplicates WHERE rn > 1

--******************7.Duplicates in Userflagged_items****************** 
WITH duplicates AS (
  SELECT
    user_flagged_id,
    ROW_NUMBER() OVER (PARTITION BY user_flagged_id ORDER BY user_flagged_id) AS rn
  FROM analytics.public.user_flagged_items
)
SELECT * FROM duplicates WHERE rn > 1

--******************8. Duplicates in Metabriteproducts********************
WITH duplicates AS (
  SELECT
    metabrite_id,
    ROW_NUMBER() OVER (PARTITION BY metabrite_id ORDER BY metabrite_id) AS rn
  FROM analytics.public.metabrite_items
)
SELECT * FROM duplicates WHERE rn > 1
