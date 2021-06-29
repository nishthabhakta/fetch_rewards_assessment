CREATE DATABASE fetch_reward; 
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS rewards_info;
CREATE TABLE rewards_info
(
uuid NVARCHAR(200) PRIMARY KEY ,
receipt_id NVARCHAR(MAX) NULL,
userId NVARCHAR(50) NULL,
brandCode NVARCHAR(MAX) NULL,
barcode NVARCHAR(MAX) NULL,
description NVARCHAR(MAX) NULL,
to_be_reviewed NVARCHAR(50) NULL,
partner_item_id NVARCHAR(50) NULL,
target_gap_points NVARCHAR(50) NULL,
purchase_quantity FLOAT NULL,
user_flagged_barcode NVARCHAR(50) NULL,
user_new_item NVARCHAR(50) NULL,
user_flagged_price FLOAT NULL,
user_flagged_quantity FLOAT NULL,
fetch_review_reason NVARCHAR(100) NULL,
no_point_reason NVARCHAR(MAX) NULL,
point_payer_id NVARCHAR(50) NULL,
rewards_group NVARCHAR(MAX) NULL,
reward_product_partner_id NVARCHAR(50) NULL,
user_flagged_desc NVARCHAR(MAX) NULL,
metabrite_barcode NVARCHAR(MAX) NULL,
metabrite_desc NVARCHAR(MAX) NULL,
competitor_reward_group NVARCHAR(MAX) NULL,
discount_price FLOAT NULL,
receipt_item_text NVARCHAR(MAX) NULL,
item_number FLOAT NULL,
metabrite_quantity_purchased FLOAT NULL,
points_earned FLOAT NULL,
target_price FLOAT NULL,
competitive_product NVARCHAR(MAX) NULL,
original_final_price FLOAT NULL,
original_metabrite_price FLOAT NULL,
deleted NVARCHAR(50) NULL,
price_after_coupon FLOAT NULL,
item_list FLOAT NULL,
metabrite_campaign_id NVARCHAR(200) NULL
);

SELECT * FROM rewards_info;

-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS receipts_info;
CREATE TABLE receipts_info
(
uuid	NVARCHAR(200) PRIMARY KEY,
receipt_id	NVARCHAR(MAX) NULL,
userId	NVARCHAR(50) NULL,
pointsEarned NUMERIC NULL,
bonusPointsEarned	NVARCHAR(MAX) NULL,
bonusPointsEarnedReason	NVARCHAR(MAX) NULL,
createDate	DATETIME2 NULL,
dateScanned	DATETIME2 NULL,
finishedDate	DATETIME2 NULL,
modifyDate	 DATETIME2 NULL,
pointsAwardedDate	DATETIME2 NULL,
purchaseDate DATETIME2 NULL	,
brandCode	NVARCHAR(MAX) NULL,
barcode NVARCHAR(MAX) NULL,
description	NVARCHAR(MAX) NULL,
purchasedItemCount	FLOAT NULL,
rewardsReceiptStatus	NVARCHAR(50) NULL,
totalSpent	FLOAT NULL,
final_price	FLOAT NULL,
item_price	FLOAT NULL,

);

SELECT * FROM receipts_info;

-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS brands_info;
CREATE TABLE brands_info
(

				brand_id NVARCHAR(200) PRIMARY KEY,
                barcode NVARCHAR(MAX), 
                brandCode NVARCHAR(MAX),
                name NVARCHAR (MAX),
                topBrand NVARCHAR (MAX),
                category NVARCHAR (MAX),
                category_id NVARCHAR(50),
				reference_id NVARCHAR(200) 
);

SELECT * FROM brands_info;
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------


DROP TABLE IF EXISTS category
CREATE TABLE category
(

			category NVARCHAR(200),
            category_id NVARCHAR(50) PRIMARY KEY
);

SELECT * FROM category;


-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS users;
CREATE TABLE users (
			userId NVARCHAR(50) PRIMARY KEY, 
            active NVARCHAR(50) NULL, 
            createdDate DATETIME2 NULL,
            lastLogin DATETIME2 NULL,
            role NVARCHAR(50) NULL,
            signUpSource NVARCHAR(50) NULL,
            state NVARCHAR(50) NULL
				);

SELECT * FROM users;
-----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS reference
CREATE TABLE reference
(
reference_type NVARCHAR(200),
reference_id NVARCHAR(200), 	
reference_type_id nvarchar(50)
);
SELECT * FROM reference
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS reference_types
CREATE TABLE reference_types 
(
type_id NVARCHAR(50) PRIMARY KEY,
reference_type NVARCHAR(100)
);
INSERT INTO reference_types values('1A','Cpgs');
INSERT INTO reference_types values('2B','Cogs');
-----------------------------------------------------------------------------------------




