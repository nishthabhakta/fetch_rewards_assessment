
--Altering the brands table--

		UPDATE brands_info
		SET
		brands_info.category_id=c.category_id
		FROM brands_info 
		INNER JOIN
		category c
		ON brands_info.category=c.category;

		UPDATE brands_info
		SET topBrand='True'
		WHERE topBrand='1';

		UPDATE brands_info 
		SET topBrand='False'
		WHERE topBrand='0';

		ALTER TABLE brands_info 
		DROP COLUMN category;

-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
		--Adding foreign key relationships:--

ALTER TABLE rewards_info WITH NOCHECK 
ADD CONSTRAINT rewards_receipt_FK FOREIGN KEY(uuid)
REFERENCES receipts_info (uuid)

ALTER TABLE rewards_info WITH NOCHECK 
ADD CONSTRAINT user_reward_foreign_key FOREIGN KEY(userId)
REFERENCES users (userId)

ALTER TABLE receipts_info WITH NOCHECK 
ADD CONSTRAINT user_receipts_foreign_key FOREIGN KEY(userId)
REFERENCES users (userId)

ALTER TABLE brands_info WITH NOCHECK 
ADD CONSTRAINT brand_category FOREIGN KEY(category_id)
REFERENCES category (category_id)

ALTER TABLE reference WITH NOCHECK 
ADD CONSTRAINT reference_type_fk FOREIGN KEY(reference_type_id)
REFERENCES reference_types (type_id)