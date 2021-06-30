# fetch_rewards_assessment

The dats is related to fetch rewards and is provided by Fetch Rewards for the assessment related to data anlysis and engineering. Three json files were processed for this assessment. Description of the files is below:

File 1: receipts.json

    _id: uuid for this receipt
    bonusPointsEarned: Number of bonus points that were awarded upon receipt completion
    bonusPointsEarnedReason: event that triggered bonus points
    createDate: The date that the event was created
    dateScanned: Date that the user scanned their receipt
    finishedDate: Date that the receipt finished processing
    modifyDate: The date the event was modified
    pointsAwardedDate: The date we awarded points for the transaction
    pointsEarned: The number of points earned for the receipt
    purchaseDate: the date of the purchase
    purchasedItemCount: Count of number of items on the receipt
    rewardsReceiptItemList: The items that were purchased on the receipt
    rewardsReceiptStatus: status of the receipt through receipt validation and processing
    totalSpent: The total amount on the receipt
    userId: string id back to the User collection for the user who scanned the receipt

File 2: brands.json

    _id: brand uuid
    barcode: the barcode on the item
    brandCode: String that corresponds with the brand column in a partner product file
    category: The category name for which the brand sells products in
    categoryCode: The category code that references a BrandCategory
    cpg: reference to CPG collection
    topBrand: Boolean indicator for whether the brand should be featured as a 'top brand'
    name: Brand name

File 3: users.json

    _id: uuid for this receipt
    bonusPointsEarned: Number of bonus points that were awarded upon receipt completion
    bonusPointsEarnedReason: event that triggered bonus points
    createDate: The date that the event was created
    dateScanned: Date that the user scanned their receipt
    finishedDate: Date that the receipt finished processing
    modifyDate: The date the event was modified
    pointsAwardedDate: The date we awarded points for the transaction
    pointsEarned: The number of points earned for the receipt
    purchaseDate: the date of the purchase
    purchasedItemCount: Count of number of items on the receipt
    rewardsReceiptItemList: The items that were purchased on the receipt
    rewardsReceiptStatus: status of the receipt through receipt validation and processing
    totalSpent: The total amount on the receipt
    userId: string id back to the User collection for the user who scanned the receipt
    
Before running the main python file, use the CreateTableStatement.sql to create the database and tables. The next step is to populate the tables with clean and processed data. 

The python file contains main code to clean and ingest data into the database. To transform and process the files follow the following steps:
1. Open cmd(Command Line)
2. Give the path for Main.py file and mention the name of files to be processed. Example:  C:\Users\nishtha\main.py receipts.json 
 
File processing description:

The files contain nested json objects and hence, json_normalize is used to flatten certain columns of the file.
There are date columns in brands.json and users.json. These dates are in a thirteen digit unix timestamp which is then converted into a standardize datetime format using datetime functions in python.
Below is a code snippet to understand how the date conversion is working


    users['createdDate'] = users['createdDate'].astype(str)
    users['createdDate']=users['createdDate'].str.replace(r"{|$|:|'|}|date|\D",'',regex=True)
    
    users['createdDate']=users['createdDate'].replace(r'^\s*$', np.nan, regex=True)
    users['createdDate']=users['createdDate'].fillna(0)
    users['createdDate']=users['createdDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    
This code will first convert the column to type string, then using the str.replace function, remove all the special characters from the date columns. Later the null values are filled and then datetime function is used.
    
The receipts.json table has multiple json objects that were normalized using the same json_normalize technique.
One specific column called rewardsReceiptItemList was observed to have many different nested json objects which were than converted to columns and dataframe. Below is a list of columns that were obtained after flattening the json object.

•	barcode	
•	description	
•	final_price	
•	item_price	
•	to_be_reviewed	
•	partner_item_id	
•	target_gap_points	
•	purchase_quantity	
•	user_flagged_barcode	
•	user_new_item	
•	user_flagged_price	
•	user_flagged_quantity	
•	fetch_review_reason	
•	no_point_reason	
•	point_payer_id	
•	rewards_group	
•	reward_product_partner_id	
•	user_flagged_desc	
•	metabrite_barcode	
•	metabrite_desc	
•	brandCode	
•	competitor_reward_group	
•	discount_price	
•	receipt_item_text	
•	item_number	
•	metabrite_quantity_purchased	
•	points_earned	
•	target_price	
•	competitive_product	
•	original_final_price
•	original_metabrite_price	
•	deleted	
•	price_after_coupon	
•	item_list	
•	metabrite_campaign_id	

Database Processes:

Once all the data is ingested by processing all the files one by one using the cmd, use the AlterTableStatement.sql to alter tables and establish foreign key relationships in SQL Server Management Studio. Run all the commands from AlterTableStatement.sql only after the data is ingested in the database.

After establishing the foreign key relationships, the database would be normalized. The ER Diagram can be found in the file ER Diagram.pdf


SQL Queries and reports
Use the Queries.sql file to run all the queries and see the report.







 
 
