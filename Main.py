#!/usr/bin/env python
# coding: utf-8

# In[48]:


import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
from pandas import Timestamp
#mport flat_table
from pandas.io.json import json_normalize 
from sqlalchemy import create_engine
import re
import sqlalchemy as sa
import urllib
import pyodbc
import sys
from uuid import uuid4

#input_path = sys.argv[1]
input_path="C://Resume Updates//Screening Calls//FetchRewards//users.json"

def users_transformation(file_path):
    print("**********Reading the json file and filling null values using pandas**********")
    users=pd.read_json(file_path, lines=True)
    users.fillna(0)
    
    print("**********Normalizing the _id column using json_normalize**********")
    
    users['_id']=json_normalize(users['_id'])
    users.head(2)
    
    print("**********Cleaning the date columns by removing special characters**********")
    
    users['createdDate'] = users['createdDate'].astype(str)
    users['createdDate']=users['createdDate'].str.replace(r"{|$|:|'|}|date|\D",'',regex=True)

    users['lastLogin'] = users['lastLogin'].astype(str)
    users['lastLogin']=users['lastLogin'].str.replace(r"{|$|:|'|}|date|\D",'',regex=True)
    
    
    print("**********Converting the 13 digit unix timestamp to datetime format**********")

    users['lastLogin']=users['lastLogin'].replace(r'^\s*$', np.nan, regex=True)
    users['lastLogin']=users['lastLogin'].fillna(0)
    users['lastLogin']=users['lastLogin'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    users['createdDate']=users['createdDate'].replace(r'^\s*$', np.nan, regex=True)
    users['createdDate']=users['createdDate'].fillna(0)
    users['createdDate']=users['createdDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    print("**********Renaming the _id column to user_id and dropping duplicates to clean dataframe and ingest in database**********")
    
    users=users.rename(columns={"_id": "userId"})
    users.duplicated(subset=['userId'])

    user=users.drop_duplicates(subset=['userId'])
    user_sql = user.fillna(value=0)
    user_sql.head(10)

    return user_sql


def category_transformation(file_path):
    
    print("**********Reading the json file and filling null values using pandas**********")
    brands=pd.read_json(file_path,lines=True, convert_dates=True, convert_axes=True,orient="columns")
    brands.head(2)
    
    
    print("**********Using only the category column from main dataframe (brands) to identify unique brand catgeories in order to normalize the database**********")

    category=brands[['category']]
    category=category.drop_duplicates()
    
    print("**********Dropping out the null values and giving a unique id to each row of category**********")
    
    category=category.dropna()
    category['category_id']=category.index+1
    
    return category
    
    
def reference_transformation(file_path):
    
    brands=pd.read_json(file_path,lines=True, convert_dates=True, convert_axes=True,orient="columns")
    brands.head(2)
    print("**********Normalizing the cpg(nested json column) using json_normalize and storing it in dataframe called reference**********")

    reference=json_normalize(brands['cpg'])
    
    print("**********Dropping out the null values and giving a unique id to each row of category**********")
    
    reference.rename(columns={'$id.$oid':'reference_id','$ref':'reference_type'}, inplace=True)
    
    return reference




def brands_transformation(file_path):
    
    print("**********Reading the json file and filling null values using pandas**********")
    brands=pd.read_json(file_path,lines=True, convert_dates=True, convert_axes=True,orient="columns")
    brands.head(2)
    
    print("**********Normalizing the _id and cpg and renaming _id to brand_id column using json normalize**********")
    
    
    brands['_id']=json_normalize(brands['_id'])
    brands=brands.rename(columns={'_id':'brand_id'})
    reference=json_normalize(brands['cpg'])
    
    
    reference.rename(columns={'$id.$oid':'reference_id','$ref':'reference_type'}, inplace=True)
    print("**********Using only the category column from main dataframe (brands) to identify unique brand catgeories in order to normalize the database**********")

    category=brands[['category']]
    category=category.drop_duplicates()
    
    print("**********Dropping out the null values and giving a unique id to each row of category**********")
    category=category.dropna()
    category['category_id']=category.index+1
    print("**********Dropping the category and cpg columns**********")
    
    brands=brands.drop(['categoryCode','cpg'], axis=1)

    print("**********Adding an empty column category_id to establish foreign key relationship in database and filling out the null values in columns to ingest clean data  in database*********")

    brands=brands.drop_duplicates(subset='brand_id')
    brands['category_id']=""
    brands['topBrand'] = brands['topBrand'].fillna('no data')


    print("**********Concating the reference csv file to the cleaned brands dataframe to give reference ids to each row and dropping th reference_type columns**********")
    
    brand=pd.concat([brands,reference], axis=1)
    brand=brand.drop(['reference_type'],axis=1)


    return brand



def receipts_transformation(file_path):

    print("**********Reading the json files using pd.read_json and filling out the null values with 0**********")
    
    receipts=pd.read_json(file_path,lines=True, convert_dates=True, convert_axes=True,orient="columns")
    receipts.shape
    receipts.fillna(0)
    receipts.head(5)
    
    
    print("**********Normalizing column of _id using json_normalize**********")
    
    receipts['_id']=json_normalize(receipts['_id'])
    
    print("**********Normalizing the rewardsReceiptItemList column which is a nested json object in a dataframe**********")
    
    receipts['rewardsReceiptItemListId']=receipts.index+1
    receipts.head(2)
    rewardsReceiptItemList=receipts.explode('rewardsReceiptItemList')
    rewardsReceiptItemList=rewardsReceiptItemList[['_id','rewardsReceiptItemListId','rewardsReceiptItemList']]
    rewardsReceiptItemList
    reward_items=pd.json_normalize(json.loads(rewardsReceiptItemList.to_json(orient='records')))
    
    print("********** Dropping the rewardsReceiptItemListId column and renaming the columns in rewards_item dataframe and later dropping null values and filling empty rows with 0 using fillna **********")
    
    reward_items=reward_items.drop(columns={'rewardsReceiptItemListId'})
    reward_items=reward_items.rename(columns={
                    '_id':'receipt_reward_id',
                    'rewardsReceiptItemList.barcode':'barcode',
                    'rewardsReceiptItemList.description':'description',
           'rewardsReceiptItemList.finalPrice':'final_price','rewardsReceiptItemList.itemPrice':'item_price',
           'rewardsReceiptItemList.needsFetchReview':'to_be_reviewed',
           'rewardsReceiptItemList.partnerItemId':'partner_item_id',
           'rewardsReceiptItemList.preventTargetGapPoints':'target_gap_points',
           'rewardsReceiptItemList.quantityPurchased':'purchase_quantity',
           'rewardsReceiptItemList.userFlaggedBarcode':'user_flagged_barcode',         
           'rewardsReceiptItemList.userFlaggedNewItem':'user_new_item',
            'rewardsReceiptItemList.userFlaggedPrice':'user_flagged_price',
            'rewardsReceiptItemList.userFlaggedQuantity':'user_flagged_quantity' ,
            'rewardsReceiptItemList.needsFetchReviewReason':'fetch_review_reason',
           'rewardsReceiptItemList.pointsNotAwardedReason':'no_point_reason',
           'rewardsReceiptItemList.pointsPayerId':'point_payer_id',
           'rewardsReceiptItemList.rewardsGroup':'rewards_group',
           'rewardsReceiptItemList.rewardsProductPartnerId':'reward_product_partner_id',
           'rewardsReceiptItemList.userFlaggedDescription':'user_flagged_desc',
           'rewardsReceiptItemList.originalMetaBriteBarcode':'metabrite_barcode',
           'rewardsReceiptItemList.originalMetaBriteDescription':'metabrite_desc',
           'rewardsReceiptItemList.brandCode':'brandCode',
           'rewardsReceiptItemList.competitorRewardsGroup':'competitor_reward_group',
           'rewardsReceiptItemList.discountedItemPrice':'discount_price',
           'rewardsReceiptItemList.originalReceiptItemText':'receipt_item_text',
           'rewardsReceiptItemList.itemNumber':'item_number',
           'rewardsReceiptItemList.originalMetaBriteQuantityPurchased':'metabrite_quantity_purchased',
           'rewardsReceiptItemList.pointsEarned':'points_earned',
           'rewardsReceiptItemList.targetPrice':'target_price',
           'rewardsReceiptItemList.competitiveProduct':'competitive_product',
           'rewardsReceiptItemList.originalFinalPrice':'original_final_price',
           'rewardsReceiptItemList.originalMetaBriteItemPrice':'original_metabrite_price',
           'rewardsReceiptItemList.deleted':'deleted',
           'rewardsReceiptItemList.priceAfterCoupon':'price_after_coupon',
           'rewardsReceiptItemList':'item_list',
           'rewardsReceiptItemList.metabriteCampaignId':'metabrite_campaign_id'

                   }
          )
    
    reward_items.head(2)
    #reward_items.drop_duplicates()
    reward_items.fillna(0)
    
    print("**********Manipulating and normalizing the main dataframe and dropping the normalized column rewardsReceiptItemList and renaming the _id column to receipt_id**********")
    
    receipts=receipts.drop(columns=['rewardsReceiptItemList','rewardsReceiptItemListId'])
    receipts=receipts.rename(columns={'_id':'receipt_id'})
    #receipts.drop_duplicates()
    
    
    print("********** Using regex to remove characters from all the date columns using the following code**********")      
    
    receipts['purchaseDate'] = receipts['purchaseDate'].astype(str)
    receipts['purchaseDate']=receipts['purchaseDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)
    
    receipts['createDate'] = receipts['createDate'].astype(str)
    receipts['createDate']=receipts['createDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)    
    
    receipts['dateScanned'] = receipts['dateScanned'].astype(str)
    receipts['dateScanned']=receipts['dateScanned'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)  
    
    receipts['finishedDate'] = receipts['finishedDate'].astype(str)
    receipts['finishedDate']=receipts['finishedDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)  
    
    receipts['modifyDate'] = receipts['modifyDate'].astype(str)
    receipts['modifyDate']=receipts['modifyDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True) 
    
    receipts['pointsAwardedDate'] = receipts['pointsAwardedDate'].astype(str)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)
    
    print("**********The heads are in a 13 digit unix time format and hence they are to be converted using the following code for all the date columns **********")
    
    receipts['purchaseDate']=receipts['purchaseDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['purchaseDate']=receipts['purchaseDate'].fillna(0)
    receipts['purchaseDate']=receipts['purchaseDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    receipts['createDate']=receipts['createDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['createDate']=receipts['createDate'].fillna(0)
    receipts['createDate']=receipts['createDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    receipts['dateScanned']=receipts['dateScanned'].replace(r'^\s*$', np.nan, regex=True)
    receipts['dateScanned']=receipts['dateScanned'].fillna(0)
    receipts['dateScanned']=receipts['dateScanned'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             

    receipts['finishedDate']=receipts['finishedDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['finishedDate']=receipts['finishedDate'].fillna(0)
    receipts['finishedDate']=receipts['finishedDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))              

    receipts['modifyDate']=receipts['modifyDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['modifyDate']=receipts['modifyDate'].fillna(0)
    receipts['modifyDate']=receipts['modifyDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             

    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].fillna(0)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             
    
    print("**********Concating rewards and receipts dataframe and adding uuid to add a unique identifier for each row**********")
    
    receipts_rewards=pd.concat([receipts,reward_items],axis=1)
    receipts_rewards['uuid'] = receipts_rewards.index.to_series().map(lambda x: uuid4())
    receipts_rewards.fillna(0)
    #receipts_rewards.drop_duplicates()  
    
    print("*********Breaking the frame to ingest into database***********")
    
    receipts_history=receipts_rewards[['uuid','receipt_id','userId','pointsEarned','bonusPointsEarned','bonusPointsEarnedReason','createDate','dateScanned','finishedDate',
                                       'modifyDate','pointsAwardedDate','purchaseDate','brandCode','barcode',
                                       'description','purchasedItemCount','rewardsReceiptStatus','totalSpent',
                                      'final_price','item_price']]    


    
    print("********* Filling out empty rows of date columns with an old date ***********")
    
    receipts_history['createDate']=receipts_history['createDate'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))
    receipts_history['dateScanned']=receipts_history['dateScanned'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))              
    receipts_history['finishedDate']=receipts_history['finishedDate'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))
    receipts_history['modifyDate']=receipts_history['modifyDate'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))              
    receipts_history['pointsAwardedDate']=receipts_history['pointsAwardedDate'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))              
    receipts_history['purchaseDate']=receipts_history['purchaseDate'].fillna(value=pd.to_datetime('1960-06-20 18:00:00'))
    
    
    return receipts_history    
    
    
def rewards_transformation(file_path):

    print("**********Reading the json files using pd.read_json and filling out the null values with 0**********")
    
    receipts=pd.read_json(file_path,lines=True, convert_dates=True, convert_axes=True,orient="columns")
    receipts.shape
    receipts.fillna(0)
    receipts.head(5)
    
    
    print("**********Normalizing column of _id using json_normalize**********")
    
    receipts['_id']=json_normalize(receipts['_id'])
    
    print("**********Normalizing the rewardsReceiptItemList column which is a nested json object in a dataframe**********")
    
    receipts['rewardsReceiptItemListId']=receipts.index+1
    receipts.head(2)
    rewardsReceiptItemList=receipts.explode('rewardsReceiptItemList')
    rewardsReceiptItemList=rewardsReceiptItemList[['_id','rewardsReceiptItemListId','rewardsReceiptItemList']]
    rewardsReceiptItemList
    reward_items=pd.json_normalize(json.loads(rewardsReceiptItemList.to_json(orient='records')))
    
    print("********** Dropping the rewardsReceiptItemListId column and renaming the columns in rewards_item dataframe and later dropping null values and filling empty rows with 0 using fillna **********")
    
    reward_items=reward_items.drop(columns={'rewardsReceiptItemListId'})
    reward_items=reward_items.rename(columns={
                    '_id':'receipt_reward_id',
                    'rewardsReceiptItemList.barcode':'barcode',
                    'rewardsReceiptItemList.description':'description',
           'rewardsReceiptItemList.finalPrice':'final_price','rewardsReceiptItemList.itemPrice':'item_price',
           'rewardsReceiptItemList.needsFetchReview':'to_be_reviewed',
           'rewardsReceiptItemList.partnerItemId':'partner_item_id',
           'rewardsReceiptItemList.preventTargetGapPoints':'target_gap_points',
           'rewardsReceiptItemList.quantityPurchased':'purchase_quantity',
           'rewardsReceiptItemList.userFlaggedBarcode':'user_flagged_barcode',         
           'rewardsReceiptItemList.userFlaggedNewItem':'user_new_item',
            'rewardsReceiptItemList.userFlaggedPrice':'user_flagged_price',
            'rewardsReceiptItemList.userFlaggedQuantity':'user_flagged_quantity' ,
            'rewardsReceiptItemList.needsFetchReviewReason':'fetch_review_reason',
           'rewardsReceiptItemList.pointsNotAwardedReason':'no_point_reason',
           'rewardsReceiptItemList.pointsPayerId':'point_payer_id',
           'rewardsReceiptItemList.rewardsGroup':'rewards_group',
           'rewardsReceiptItemList.rewardsProductPartnerId':'reward_product_partner_id',
           'rewardsReceiptItemList.userFlaggedDescription':'user_flagged_desc',
           'rewardsReceiptItemList.originalMetaBriteBarcode':'metabrite_barcode',
           'rewardsReceiptItemList.originalMetaBriteDescription':'metabrite_desc',
           'rewardsReceiptItemList.brandCode':'brandCode',
           'rewardsReceiptItemList.competitorRewardsGroup':'competitor_reward_group',
           'rewardsReceiptItemList.discountedItemPrice':'discount_price',
           'rewardsReceiptItemList.originalReceiptItemText':'receipt_item_text',
           'rewardsReceiptItemList.itemNumber':'item_number',
           'rewardsReceiptItemList.originalMetaBriteQuantityPurchased':'metabrite_quantity_purchased',
           'rewardsReceiptItemList.pointsEarned':'points_earned',
           'rewardsReceiptItemList.targetPrice':'target_price',
           'rewardsReceiptItemList.competitiveProduct':'competitive_product',
           'rewardsReceiptItemList.originalFinalPrice':'original_final_price',
           'rewardsReceiptItemList.originalMetaBriteItemPrice':'original_metabrite_price',
           'rewardsReceiptItemList.deleted':'deleted',
           'rewardsReceiptItemList.priceAfterCoupon':'price_after_coupon',
           'rewardsReceiptItemList':'item_list',
           'rewardsReceiptItemList.metabriteCampaignId':'metabrite_campaign_id'

                   }
          )
    
    reward_items.head(2)
    #reward_items.drop_duplicates()
    reward_items.fillna(0)
    
    print("**********Manipulating and normalizing the main dataframe and dropping the normalized column rewardsReceiptItemList and renaming the _id column to receipt_id**********")
    
    receipts=receipts.drop(columns=['rewardsReceiptItemList','rewardsReceiptItemListId'])
    receipts=receipts.rename(columns={'_id':'receipt_id'})
    #receipts.drop_duplicates()
    
    
    print("********** Using regex to remove characters from all the date columns using the following code**********")      
    
    receipts['purchaseDate'] = receipts['purchaseDate'].astype(str)
    receipts['purchaseDate']=receipts['purchaseDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)
    
    receipts['createDate'] = receipts['createDate'].astype(str)
    receipts['createDate']=receipts['createDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)    
    
    receipts['dateScanned'] = receipts['dateScanned'].astype(str)
    receipts['dateScanned']=receipts['dateScanned'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)  
    
    receipts['finishedDate'] = receipts['finishedDate'].astype(str)
    receipts['finishedDate']=receipts['finishedDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)  
    
    receipts['modifyDate'] = receipts['modifyDate'].astype(str)
    receipts['modifyDate']=receipts['modifyDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True) 
    
    receipts['pointsAwardedDate'] = receipts['pointsAwardedDate'].astype(str)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].str.replace(r"{|$|:|'|}|date|\D| ",'',regex=True)
    
    print("**********The heads are in a 13 digit unix time format and hence they are to be converted using the following code for all the date columns **********")
    
    receipts['purchaseDate']=receipts['purchaseDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['purchaseDate']=receipts['purchaseDate'].fillna(0)
    receipts['purchaseDate']=receipts['purchaseDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    receipts['createDate']=receipts['createDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['createDate']=receipts['createDate'].fillna(0)
    receipts['createDate']=receipts['createDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

    receipts['dateScanned']=receipts['dateScanned'].replace(r'^\s*$', np.nan, regex=True)
    receipts['dateScanned']=receipts['dateScanned'].fillna(0)
    receipts['dateScanned']=receipts['dateScanned'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             

    receipts['finishedDate']=receipts['finishedDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['finishedDate']=receipts['finishedDate'].fillna(0)
    receipts['finishedDate']=receipts['finishedDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))              

    receipts['modifyDate']=receipts['modifyDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['modifyDate']=receipts['modifyDate'].fillna(0)
    receipts['modifyDate']=receipts['modifyDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             

    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].replace(r'^\s*$', np.nan, regex=True)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].fillna(0)
    receipts['pointsAwardedDate']=receipts['pointsAwardedDate'].apply(lambda d: datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))             
    
    print("**********Concating rewards and receipts dataframe and adding uuid to add a unique identifier for each row**********")
    
    receipts_rewards=pd.concat([receipts,reward_items],axis=1)
    receipts_rewards['uuid'] = receipts_rewards.index.to_series().map(lambda x: uuid4())
    receipts_rewards.fillna(0)
    #receipts_rewards.drop_duplicates()  
    
    print("*********Breaking the frame to ingest into database***********")
    
    
    
    rewards_history=receipts_rewards[['uuid','receipt_id','userId','brandCode','barcode','description','to_be_reviewed','partner_item_id',
                      'target_gap_points','purchase_quantity','user_flagged_barcode','user_new_item',
                      'user_flagged_price','user_flagged_quantity','fetch_review_reason','no_point_reason',
                      'point_payer_id','rewards_group','reward_product_partner_id','user_flagged_desc',
                      'metabrite_barcode','metabrite_desc','competitor_reward_group',
                      'discount_price','receipt_item_text','item_number',
                      'metabrite_quantity_purchased','points_earned','target_price','competitive_product','original_final_price',
                     'original_metabrite_price','deleted','price_after_coupon','item_list','metabrite_campaign_id'

                     ]]
    rewards_history.reset_index(drop=True)
    rewards_history=rewards_history.fillna(0) 
    rewards_history=rewards_history.astype(str)
    rewards_history.info()
    
    return rewards_history
  

if input_path.split("//")[-1].split(".")[0] == 'receipts':
    receipts_transformation =  receipts_transformation(input_path)
    print(receipts_transformation)

    #receipts_rewards_transformation=receipts_rewards_transformation.astype(str)
    receipts_transformation=receipts_transformation.fillna(0)
    #receipts_history=receipts_history.astype(str)
    conn_str = (
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
        r'DATABASE=fetch_reward;'
        r'Trusted_Connection=yes;'
    )
    cnxn = pyodbc.connect(conn_str)

    cursor = cnxn.cursor()
    cursor.execute(" Truncate table receipts_info;")
    for index, row in receipts_transformation.iterrows():
           cursor.execute('INSERT INTO dbo.receipts_info([uuid],[receipt_id],[userId],[pointsEarned],[bonusPointsEarned],[bonusPointsEarnedReason],[createDate],[dateScanned],[finishedDate],[modifyDate],[pointsAwardedDate],[purchaseDate],[brandCode],[barcode],[description],[purchasedItemCount],[rewardsReceiptStatus],[totalSpent],[final_price],[item_price]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            row['uuid'],
                            row['receipt_id'],
                            row['userId'],
                            row['pointsEarned'],
                            row['bonusPointsEarned'],
                            row['bonusPointsEarnedReason'],
                            row['createDate'],
                            row['dateScanned'],
                            row['finishedDate'],
                            row['modifyDate'],
                            row['pointsAwardedDate'],
                            row['purchaseDate'],
                            row['brandCode'],
                            row['barcode'],
                            row['description'],
                            row['purchasedItemCount'],
                            row['rewardsReceiptStatus'],
                            row['totalSpent'],
                            row['final_price'],
                            row['item_price']
                         )
            
   
    cnxn.commit()
    cursor.close()
    #cnxn.close()


#Rewards Ingestion

if input_path.split("//")[-1].split(".")[0] == 'receipts':
    rewards_transformation =  rewards_transformation(input_path)
    print(rewards_transformation)

    #receipts_rewards_transformation=receipts_rewards_transformation.astype(str)
    rewards_transformation=rewards_transformation.fillna(0)
    #receipts_history=receipts_history.astype(str)
    conn_str = (
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
        r'DATABASE=fetch_reward;'
        r'Trusted_Connection=yes;'
    )
    curr = pyodbc.connect(conn_str)

    cursor = curr.cursor()
    cursor.execute(" Truncate table rewards_info;")
    for index,row in rewards_transformation.iterrows():
        #print(index,row)
        cursor.execute('INSERT INTO dbo.rewards_info([uuid],[receipt_id],[userId],[brandCode],[barcode],[description],[to_be_reviewed],[partner_item_id],[target_gap_points],[purchase_quantity],[user_flagged_barcode],[user_new_item], [user_flagged_price],[user_flagged_quantity],[fetch_review_reason],[no_point_reason],[point_payer_id],[rewards_group],[reward_product_partner_id],[user_flagged_desc], [metabrite_barcode],[metabrite_desc],[competitor_reward_group],[discount_price],[receipt_item_text],[item_number],[metabrite_quantity_purchased],[points_earned],[target_price],[competitive_product],[original_final_price],[original_metabrite_price],[deleted],[price_after_coupon],[item_list],[metabrite_campaign_id]) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            row['uuid'],
                            row['receipt_id'],
                            row['userId'],
                            row['brandCode'],
                            row['barcode'],
                            row['description'],
                            row['to_be_reviewed'],
                            row['partner_item_id'],
                            row['target_gap_points'],
                            row['purchase_quantity'],
                            row['user_flagged_barcode'],
                            row['user_new_item'], 
                            row['user_flagged_price'],
                            row['user_flagged_quantity'],
                            row['fetch_review_reason'],
                            row['no_point_reason'],
                            row['point_payer_id'],
                            row['rewards_group'],
                            row['reward_product_partner_id'],
                            row['user_flagged_desc'], 
                            row['metabrite_barcode'],
                            row['metabrite_desc'],
                            row['competitor_reward_group'],
                            row['discount_price'],
                            row['receipt_item_text'],
                            row['item_number'],
                            row['metabrite_quantity_purchased'],
                            row['points_earned'],
                            row['target_price'],
                            row['competitive_product'],
                            row['original_final_price'],
                            row['original_metabrite_price'],
                            row['deleted'],
                            row['price_after_coupon'],
                            row['item_list'],
                            row['metabrite_campaign_id']
                      )

    cnxn.commit()
    cursor.close()
    cnxn.close()

    

if input_path.split("//")[-1].split(".")[0] == 'users':
    users_transformation =  users_transformation(input_path)
    print(users_transformation)
    
    conn_str = (
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
        r'DATABASE=fetch_reward;'
        r'Trusted_Connection=yes;'
    )
    cnxn = pyodbc.connect(conn_str)

    cursor = cnxn.cursor()

    for index,row in users_transformation.iterrows():
        cursor.execute('INSERT INTO dbo.users([userId],[active],[createdDate],[lastLogin],[role],[signUpSource],[state]) values (?,?,?,?,?,?,?)',
                        row['userId'], 
                        row['active'], 
                        row['createdDate'],
                        row['lastLogin'],
                       row['role'],
                        row['signUpSource'],
                        row['state']


                      )
    cnxn.commit()
    #cursor.close()
    #cnxn.close()
    
    

   
    #Brands
    
if input_path.split("//")[-1].split(".")[0] == 'brands':
    brands_transformation =  brands_transformation(input_path)
    print(brands_transformation)
    
    
    brands_transformation=brands_transformation.fillna(0)
    conn_str = (
    r'DRIVER={SQL Server Native Client 11.0};'
    r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
    r'DATABASE=fetch_reward;'
    r'Trusted_Connection=yes;'
    )
    cnxn = pyodbc.connect(conn_str)

    cursor = cnxn.cursor()
    cursor.execute(" Truncate table brands_info;")

    for index,row in brands_transformation.iterrows():
        cursor.execute('INSERT INTO dbo.brands_info([brand_id],[barcode],[brandCode],[name],[topBrand],[category],[category_id],[reference_id]) values (?,?,?,?,?,?,?,?)', 
                        row['brand_id'], 
                        row['barcode'], 
                        row['brandCode'],
                        row['name'],
                        row['topBrand'],
                        row['category'],
                        row['category_id'],
                        row['reference_id'])

    cnxn.commit()
    #cursor.close()
    #cnxn.close()
    
#Category
if input_path.split("//")[-1].split(".")[0] == 'brands':
    category_transformation =  category_transformation(input_path)
    print(category_transformation)
          
    conn_str = (
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
        r'DATABASE=fetch_reward;'
        r'Trusted_Connection=yes;'
    )
    cursor = cnxn.cursor()
    cursor.execute(" Truncate table category;")
    #sql = "INSERT INTO dbo.category([category],[category_id]) values (?,?))"
    #sql_cols = ['category','category_id']
    #cursor.executemany(sql, category_transformation[sql_cols].values.tolist())   
    #cnxn.commit()
    #cursor.close()
    #cnxn.close()
    
    for index,row in category_transformation.iterrows():
        #print(index,row)
        cursor.execute('INSERT INTO dbo.category([category],[category_id]) values (?,?)', 
                        row['category'], 
                        row['category_id']
                       )
    curr.commit()
    #cursor.close()
    #curr.close()
    
    

#References
if input_path.split("//")[-1].split(".")[0] == 'brands':
    reference_transformation =  reference_transformation(input_path)
    print(reference_transformation)
          
    conn_str = (
        r'DRIVER={SQL Server Native Client 11.0};'
        r'SERVER=DESKTOP-0IT49LB\SQLEXPRESS;'
        r'DATABASE=fetch_reward;'
        r'Trusted_Connection=yes;'
    )
    curr = pyodbc.connect(conn_str)

    cursor = curr.cursor()
    cursor.execute(" Truncate table reference;")
    for index,row in reference_transformation.iterrows():
        #print(index,row)
        cursor.execute('INSERT INTO dbo.reference([reference_type],[reference_id]) values (?,?)', 
                        row['reference_type'], 
                        row['reference_id']
                       )
    curr.commit()
    cursor.close()
    curr.close()

 
    
    


# In[28]:





# In[ ]:





# In[ ]:




