/* What are the top 5 brands by receipts scanned for most recent month? */

WITH base AS (
SELECT b.name as brand_name,
	   r.dateScanned 
FROM brands_info b INNER JOIN receipts_info r
ON b.brandCode=r.brandCode
WHERE MONTH(GETDATE()) = MONTH(r.dateScanned)
--AND year(GETDATE()) = year(r.dateScanned)
),
baseb as (

SELECT a.brand_name as Brand_Name,
	   COUNT(*) count_receipts FROM base a
GROUP BY a.brand_name
ORDER BY COUNT(*) DESC
OFFSET 0 ROWS
FETCH NEXT 5 ROWS ONLY
)

SELECT Brand_Name from baseb

------------------------------------------------------------------------------------------------------


/* How does the ranking of the top 5 brands by receipts scanned for
the recent month compare to the ranking for the previous month */



WITH base AS (
SELECT b.name as brand_name,
	   r.dateScanned 
FROM brands_info b INNER JOIN receipts_info r
ON b.brandCode=r.brandCode
WHERE MONTH(GETDATE())-1 = MONTH(r.dateScanned)
)

SELECT DENSE_RANK() OVER(PARTITION BY base.brand_name ORDER BY COUNT(*) DESC) FROM base
GROUP BY base.brand_name
ORDER BY COUNT(*) DESC
OFFSET 0 ROWS
FETCH NEXT 5 ROWS ONLY;

------------------------------------------------------------------------------------------------------


/*When considering average spend from receipts with 
'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?*/


SELECT 
	SUM(CASE WHEN rewardsReceiptStatus='Finished' THEN totalSpent END)/COUNT(DISTINCT receipt_id) AS Accepted,
	SUM(CASE WHEN rewardsReceiptStatus='Flagged' THEN totalSpent END)/COUNT(DISTINCT receipt_id) AS Rejected
	FROM receipts_info;


------------------------------------------------------------------------------------------------------

/*When considering total number of items purchased from receipts 
with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? */

SELECT 
	SUM(CASE WHEN rewardsReceiptStatus='Finished' THEN purchasedItemCount END) AS Accepted,
	SUM(CASE WHEN rewardsReceiptStatus='Flagged' THEN purchasedItemCount END) AS Rejected
	FROM receipts_info;



------------------------------------------------------------------------------------------------------
/* Which brand has the most spend among users who were created within the past 6 months? */

WITH base as(
SELECT  r.totalSpent as total_amount, 
		u.UserId, 
		r.brandCode as brandcode FROM users u
INNER JOIN
receipts_info r
ON U.userId=r.userId
WHERE  MONTH(GETDATE())-5 = MONTH(u.createdDate)
),

	base2 as (
SELECT   b.name  AS Brand_Name,  
		SUM(a.total_amount) as amount_spent 
		FROM base a
INNER JOIN
brands_info b
ON a.brandcode=b.brandCode
GROUP BY b.name, a.brandcode
)

SELECT Brand_Name FROM base2

------------------------------------------------------------------------------------------------------
/* Which brand has the most transactions among users who were created within the past 6 months? */


WITH base as(
SELECT  r.receipt_id as receipts, u.UserId, r.brandCode as brandcode FROM users u
INNER JOIN
receipts_info r
ON U.userId=r.userId
WHERE  MONTH(GETDATE())-5 = MONTH(u.createdDate)

),

	base2 as (
SELECT   b.name  AS Brand_Name,  
		COUNT(DISTINCT a.receipts) as count_of_receipts FROM base a
INNER JOIN
brands_info b
ON a.brandcode=b.brandCode
GROUP BY b.name
)

SELECT b.Brand_Name from base2 b






