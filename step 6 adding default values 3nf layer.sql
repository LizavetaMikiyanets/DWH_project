CREATE OR REPLACE PROCEDURE bl_cl.insert_default_values_3nf_layer()

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
	schema_name := 'bl_3nf';
	table_name := 'all_tables';
    start_time := CURRENT_TIMESTAMP;

    -- Insert default values into bl_3nf.CE_PAYMENT_METHODS table
    INSERT INTO bl_3nf.CE_PAYMENT_METHODS
        (PAYMENT_METHOD_SURR_ID, PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_ONLINE_ADD_CHANELS table
    INSERT INTO BL_3NF.CE_ONLINE_ADD_CHANELS
        (ONLINE_ADD_CHANNEL_SURR_ID, ONLINE_ADD_CHANNEL_SRC_ID, ONLINE_ADD_CHANNEL_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_SHIPPING_METHOD table
    INSERT INTO BL_3NF.CE_SHIPPING_METHOD
        (SHIPPING_METHOD_SURR_ID, SHIPPING_METHOD_SRC_ID, SHIPPING_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_WEBS table
    INSERT INTO BL_3NF.CE_WEBS
        (WEB_SURR_ID, WEB_SRC_ID, WEB_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_CUSTOMERS table
    INSERT INTO BL_3NF.CE_CUSTOMERS
        (CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, USER_NAME, PHONE, EMAIL, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_FORMATS table
    INSERT INTO BL_3NF.CE_FORMATS
        (FORMAT_SURR_ID, FORMAT_SRC_ID, FORMAT_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

  
    -- Insert default values into BL_3NF.CE_COUNTRIES table
    INSERT INTO BL_3NF.CE_COUNTRIES
        (COUNTRY_SURR_ID, COUNTRY_SRC_ID, COUNTRY_NAME, COUNTRY_CODE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_AUTHORS table
    INSERT INTO BL_3NF.CE_AUTHORS
        (AUTHOR_SURR_ID, AUTHOR_SRC_ID, AUTHOR_FIRST_NAME, AUTHOR_LAST_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_GENRES table
    INSERT INTO BL_3NF.CE_GENRES
        (GENRE_SURR_ID, GENRE_SRC_ID, GENRE_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_BOOKS table
    INSERT INTO BL_3NF.CE_BOOKS
        (BOOK_SURR_ID, BOOK_SRC_ID, BOOK_NAME, GENRE_SRC_ID, AUTHOR_SRC_ID, FORMAT_SRC_ID, BOOKS_PRICE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', -1, -1, -1, -1, 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_STORES table
    INSERT INTO BL_3NF.CE_STORES
        (STORE_SURR_ID, STORE_SRC_ID, STORE_NAME, ADDRESS_SRC_ID, PHONE, EMAIL, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', -1, 'n. a.', 'n. a.', 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_CITIES table
    INSERT INTO BL_3NF.CE_CITIES
        (CITY_SURR_ID, CITY_SRC_ID, CITY_NAME, COUNTRY_SRC_ID, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', -1, 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

    -- Insert default values into BL_3NF.CE_STREETS table
    INSERT INTO BL_3NF.CE_STREETS
        (STREET_SURR_ID, STREET_SRC_ID, STREET_NAME, CITY_SRC_ID, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', -1, 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999');

     -- Insert default values into BL_3NF.CE_ADDRESSES_SCD2 table
    INSERT INTO BL_3NF.CE_ADDRESSES_SCD2
        (ADDRESS_SURR_ID, ADDRESS_SRC_ID, ADDRESS_NAME, STREET_SRC_ID, CITY_SRC_ID, COUNTRY_SRC_ID, SOURCE_SYSTEM, SOURCE_ENTITY, START_DT, END_DT, IS_ACTIVE, INSERT_DT, UPDATE_DT)
    VALUES
        (-1, -1, 'n. a.', -1, -1, -1, 'MANUAL', 'MANUAL', '1-1-1900', '31-12-9999', TRUE, '1-1-1900', '31-12-9999');

  
      -- Insert default values into BL_3NF.CE_SALES table

    INSERT INTO BL_3NF.CE_SALES
        (TIME_ID, STORE_SRC_ID, ADDRESS_SRC_ID, BOOK_SRC_ID, GENRE_SRC_ID, AUTHOR_SRC_ID, FORMAT_SRC_ID, BOOKS_PRICE, AMOUNT, TOTAL_SALES_AMOUNT, COUNTRY_SRC_ID, CUSTOMER_SRC_ID, PAYMENT_METHOD_SRC_ID, ONLINE_ADD_CHANNEL_SRC_ID, SHIPPING_METHOD_SRC_ID, WEB_SRC_ID)
    VALUES
        ('1-1-1900', -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);
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



CALL bl_cl.insert_default_values_3nf_layer();
