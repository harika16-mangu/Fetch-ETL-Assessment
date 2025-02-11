-- ==================== Data Quality - Null values for Analytics Database ===========================
--===================================================================================================

/* Reasons for checking Null values
1.Data Completeness: Missing values could effect accuracy of analysis and impact decision-making
2.Data Integrity: Nulls indicate data entry errors or issues with data import processes.
3.Identifying percentage of null values from each column of a table will help us handle the columns seperately*/

--******************1.Null values check for Users Table******************
SELECT
    'users' AS table_name,
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
    SUM(CASE WHEN active IS NULL THEN 1 ELSE 0 END) AS null_active,
    SUM(CASE WHEN role IS NULL THEN 1 ELSE 0 END) AS null_role,
    SUM(CASE WHEN signUpSource IS NULL THEN 1 ELSE 0 END) AS null_signUpSource,
    SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS null_state,
    SUM(CASE WHEN createdDate IS NULL THEN 1 ELSE 0 END) AS null_createdDate,
    SUM(CASE WHEN lastLogin IS NULL THEN 1 ELSE 0 END) AS null_lastLogin
FROM analytics.public.users;

--******************2.Null values check for Brands Table******************
SELECT
'brands'AS table_name,
COUNT(*)AS TOTAL_RECORDS,
SUM(CASE WHEN brand_id IS NULL THEN 1 ELSE 0 END)AS BRAND_ID,
SUM(CASE WHEN barcode IS NULL THEN 1 ELSE 0 END)AS BARCODE,
SUM(CASE WHEN brandCode='0' THEN 1 ELSE 0 END)as BRAND_CODE,
SUM(CASE WHEN category='0' THEN 1 ELSE 0 END)AS CATEGORY,
SUM(CASE WHEN categoryCode='0' THEN 1 ELSE 0 END)AS CATEGORYCODE,
SUM(CASE WHEN cpg_id IS NULL THEN 1 ELSE 0 END)AS CPG_ID,
SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END)AS BRAND_NAME,
SUM(CASE WHEN topBrand='UNKNOWN' THEN 1 ELSE 0 END)AS TOP_BRAND
FROM analytics.public.brands

--******************3.Null values check for Receipts Table******************
SELECT
'receipts' AS table_name,
COUNT(*)AS TOTAL_RECORDS,
SUM(CASE WHEN receipt_id IS NULL THEN 1 ELSE 0 END)AS RECEIPT_ID,
SUM(CASE WHEN userId IS NULL THEN 1 ELSE 0 END)AS USERID,
SUM(CASE WHEN bonusPointsEarned=0 THEN 1 ELSE 0 END)AS BONUSPOINTS,
SUM(CASE WHEN pointsEarned=0 THEN 1 ELSE 0 END)AS POINTSEARNED,
SUM(CASE WHEN purchasedItemCount=0 THEN 1 ELSE 0 END)AS ITEMCOUNT,
SUM(CASE WHEN totalSpent=0 THEN 1 ELSE 0 END)AS TOTALSPENT,
SUM(CASE WHEN rewardsReceiptStatus='UNKNOWN' THEN 1 ELSE 0 END)AS RECEIPTSTATUS,
SUM(CASE WHEN createDate IS NULL THEN 1 ELSE 0 END)AS CREATEDDATE,
SUM(CASE WHEN purchaseDate IS NULL THEN 1 ELSE 0 END)AS PURCHASEDATE,
SUM(CASE WHEN DATESCANNED IS NULL THEN 1 ELSE 0 END)AS SCANNEDDATE,
SUM(CASE WHEN MODIFYDATE IS NULL THEN 1 ELSE 0 END)AS MODIFYDATE,
SUM(CASE WHEN POINTSAWARDEDDATE IS NULL THEN 1 ELSE 0 END)AS AWARDEDDATE
FROM analytics.public.receipts

--******************4.Null values check for ReceiptItems Table******************
SELECT
'receipt_items'AS table_name,count(*)AS TOTAL_RECORDS,
SUM(CASE WHEN receipt_id IS NULL THEN 1 ELSE 0 END)AS RECEIPT_ID,
SUM(CASE WHEN barcode IS NULL THEN 1 ELSE 0 END)AS BARCODE,
SUM(CASE WHEN partneritemid IS NULL THEN 1 ELSE 0 END)AS PARTNERID,
SUM(CASE WHEN preventtargetgappoints IS NULL THEN 1 ELSE 0 END) AS TARRGETGAP,
SUM(CASE WHEN quantitypurchased IS NULL THEN 1 ELSE 0 END)AS QUANTITY,
SUM(CASE WHEN userflaggedbarcode IS NULL THEN 1 ELSE 0 END)AS USERFLAGGED,
SUM(CASE WHEN originalreceiptitemtext IS NULL THEN 1 ELSE 0 END)AS RECEIPTTEXT,
SUM(CASE WHEN originalmetabritebarcode IS NULL THEN 1 ELSE 0 END)AS METABARCODE,
SUM(CASE WHEN originalfinalprice IS NULL THEN 1 ELSE 0 END)AS FINALPRICE,
SUM(CASE WHEN rewardsproductpartnerid IS NULL THEN 1 ELSE 0 END)AS REWARDSPATNER
from RECEIPT_ITEMS;

--******************5. Null values check for products Table*******************
SELECT
'products'AS table_name,count(*)AS TOTAL_RECORDS,
SUM(CASE WHEN barcode IS NULL THEN 1 ELSE 0 END)AS BARCODE,
SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS DESCRIPTION,
SUM(CASE WHEN itemPrice IS NULL THEN 1 ELSE 0 END)AS ITEMPRICE,
SUM(CASE WHEN targetPrice IS NULL THEN 1 ELSE 0 END)AS TARGETPRICE,
SUM(CASE WHEN discountedItemPrice IS NULL THEN 1 ELSE 0 END)AS DISCOUNTPRICE,
SUM(CASE WHEN finalPrice IS NULL THEN 1 ELSE 0 END)AS FINALPRICE,
SUM(CASE WHEN competitiveProduct IS NULL THEN 1 ELSE 0 END)AS COMPETITIVE,
SUM(CASE WHEN needsFetchReview IS NULL THEN 1 ELSE 0 END)AS FETCHREVIEW,
SUM(CASE WHEN needsFetchReviewReason IS NULL THEN 1 ELSE 0 END)AS FETCHREASON,
SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)AS PRODUCTID
from PRODUCT

--******************6. Null values check for Rewards Table******************
SELECT
'REWARDS'AS table_name,count(*)AS TOTAL_RECORDS,
SUM(CASE WHEN rewardsProductPartnerId IS NULL THEN 1 ELSE 0 END)AS RPATNERID,
SUM(CASE WHEN rewardsGroup IS NULL THEN 1 ELSE 0 END)AS RGROUP,
SUM(CASE WHEN pointsEarned IS NULL THEN 1 ELSE 0 END)AS RPEARNED,
SUM(CASE WHEN pointsPayerId IS NULL THEN 1 ELSE 0 END)AS RPAYERID,
SUM(CASE WHEN pointsNotAwardedReason IS NULL THEN 1 ELSE 0 END) AS RPOINTSNOTRESON,
SUM(CASE WHEN reward_id IS NULL THEN 1 ELSE 0 END)AS RREWARDID,
from rewards

--*******************7.Null values check for UserFlaggedItems******************
SELECT
'USERFLAGGED'AS table_name,count(*)AS TOTAL_RECORDS,
SUM(CASE WHEN userFlaggedBarcode IS NULL THEN 1 ELSE 0 END)AS UBARCODE,
SUM(CASE WHEN userFlaggedDescription IS NULL THEN 1 ELSE 0 END)AS UDESCRIPTION,
SUM(CASE WHEN userFlaggedPrice IS NULL THEN 1 ELSE 0 END)AS USERFLAGGEDPRICE,
SUM(CASE WHEN userFlaggedQuantity IS NULL THEN 1 ELSE 0 END)AS UQUANTITY,
SUM(CASE WHEN userFlaggedNewItem IS NULL THEN 1 ELSE 0 END)AS UNEWITEM,
SUM(CASE WHEN user_flagged_id IS NULL THEN 1 ELSE 0 END)AS USERFID,
from user_flag_items

--******************8.Null values check for MetaBriteProducts******************
SELECT
'METABRITE'AS table_name,count(*) AS TOTAL_RECORDS,
SUM(CASE WHEN originalMetaBriteBarcode IS NULL THEN 1 ELSE 0 END)AS MBARCODE,
SUM(CASE WHEN originalMetaBriteDescription IS NULL THEN 1 ELSE 0 END)AS MDESCRIPTION,
SUM(CASE WHEN originalMetaBriteItemPrice IS NULL THEN 1 ELSE 0 END)AS MITEMPRICE,
SUM(CASE WHEN originalMetaBriteQuantityPurchased IS NULL THEN 1 ELSE 0 END)AS MQUANTITY,
SUM(CASE WHEN metabrite_id IS NULL THEN 1 ELSE 0 END)MID
from METABRITE_ITEMS




