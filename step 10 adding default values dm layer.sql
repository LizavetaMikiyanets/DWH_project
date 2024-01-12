CREATE OR REPLACE PROCEDURE bl_cl.insert_default_values_dm_layer()

LANGUAGE plpgsql
AS
$$
DECLARE

    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
	user_name  text;
	message text;
	schema_name text;
	table_name text;
BEGIN
   user_name := current_user;
	message := 'filling all tables default values completed';
	schema_name := 'bl_dm';
	table_name := 'all_tables';
    start_time := CURRENT_TIMESTAMP;

-- Insert default values into bl_dm.DIM_PAYMENT_METHODS table
INSERT INTO bl_dm.DIM_PAYMENT_METHODS
    (PAYMENT_METHOD_SURR_ID, PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_SHIPPING_METHODS table
INSERT INTO bl_dm.DIM_SHIPPING_METHODS
    (SHIPPING_METHOD_SURR_ID, SHIPPING_METHOD_SRC_ID, SHIPPING_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_ONLINE_ADD_CHANNELS table
INSERT INTO bl_dm.DIM_ONLINE_ADD_CHANNELS
    (ONLINE_ADD_CHANNEL_SURR_ID, ONLINE_ADD_CHANNEL_SRC_ID, ONLINE_ADD_CHANNEL_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_WEBS table
INSERT INTO bl_dm.DIM_WEBS
    (WEB_SURR_ID, WEB_SRC_ID, WEB_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_CUSTOMERS table
INSERT INTO bl_dm.DIM_CUSTOMERS
    (CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, USER_NAME, PHONE, EMAIL, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_STORES table
INSERT INTO bl_dm.DIM_STORES
    (STORE_SURR_ID, STORE_SRC_ID, STORE_NAME, ADDRESS_NAME, PHONE, EMAIL, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_BOOKS table
INSERT INTO bl_dm.DIM_BOOKS
    (BOOK_SURR_ID, BOOK_SRC_ID, BOOK_NAME, GENRE_NAME, AUTHOR_FIRST_NAME, AUTHOR_LAST_NAME, FORMAT_NAME, BOOKS_PRICE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31');

-- Insert default values into bl_dm.DIM_ADDRESSES_SCD2 table
INSERT INTO bl_dm.DIM_ADDRESSES_SCD2
    (ADDRESS_SURR_ID, ADDRESS_SRC_ID, ADDRESS_NAME, STREET_NAME, CITY_NAME, COUNTRY_NAME, COUNTRY_CODE, SOURCE_SYSTEM, SOURCE_ENTITY, START_DT, END_DT, IS_ACTIVE, INSERT_DT, UPDATE_DT)
VALUES
    (-1, -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1900-01-01', '9999-12-31', TRUE, '1900-01-01', '9999-12-31');

INSERT INTO bl_dm.FCT_SALES
    (TIME_ID,
  STORE_NAME,
  ADDRESS_NAME,
  BOOK_NAME,
  GENRE_NAME,
  AUTHOR_FIRST_NAME,
  AUTHOR_LAST_NAME,
  FORMAT_NAME,
  BOOKS_PRICE,
  AMOUNT,
  TOTAL_SALES_AMOUNT,
  COUNTRY_NAME,
  CUSTOMER_FIRST_NAME,
  CUSTOMER_LAST_NAME, 
  PAYMENT_METHOD_NAME,
  ONLINE_ADD_CHANNEL_NAME,
  SHIPPING_METHOD_NAME,
  WEB_NAME
)
VALUES
    ('1900-01-01', 'n. a.','n. a.','n. a.','n. a.','n. a.','n. a.','n. a.',-1, -1, -1, 'n. a.','n. a.','n. a.','n. a.','n. a.','n. a.','n. a.');

  	end_time := CURRENT_TIMESTAMP;
    run_time := end_time - start_time;

BEGIN 
CALL bl_cl.log_event(user_name, message, schema_name, table_name, start_time, end_time, run_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during loading: %', SQLERRM;
        -- Log the event before re-raising the exception
        CALL bl_cl.log_event(user_name, SQLERRM, schema_name, table_name, start_time, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - start_time);
        RAISE;
END;
END;
$$;

call bl_cl.insert_default_values_dm_layer()