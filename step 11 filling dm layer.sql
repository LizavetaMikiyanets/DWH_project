CREATE OR REPLACE PROCEDURE bl_cl.load_dim_times_table()
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
	message := 'filling DIM_TIMES table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_TIMES';
    start_time := CURRENT_TIMESTAMP;

INSERT INTO bl_dm.DIM_TIMES (TIME_ID, DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, FISCAL_WEEK_NUMBER, WEEK_ENDING_DAY, WEEK_ENDING_DAY_ID, CALENDAR_MONTH_NUMBER, FISCAL_MONTH_NUMBER, CALENDAR_MONTH_DESC, CALENDAR_MONTH_ID, FISCAL_MONTH_DESC, FISCAL_MONTH_ID, DAYS_IN_CAL_MONTH, DAYS_IN_FIS_MONTH, END_OF_CAL_MONTH, END_OF_FIS_MONTH, CALENDAR_MONTH_NAME, FISCAL_MONTH_NAME, CALENDAR_QUARTER_DESC, CALENDAR_QUARTER_ID, FISCAL_QUARTER_DESC, FISCAL_QUARTER_ID, DAYS_IN_CAL_QUARTER, DAYS_IN_FIS_QUARTER, END_OF_CAL_QUARTER, END_OF_FIS_QUARTER, CALENDAR_QUARTER_NUMBER, FISCAL_QUARTER_NUMBER, CALENDAR_YEAR, CALENDAR_YEAR_ID, FISCAL_YEAR, FISCAL_YEAR_ID, DAYS_IN_CAL_YEAR, DAYS_IN_FIS_YEAR, END_OF_CAL_YEAR, END_OF_FIS_YEAR)
SELECT
    TIME_ID,
    TO_CHAR(TIME_ID, 'DAY') AS DAY_NAME,
    EXTRACT(ISODOW FROM TIME_ID)::INT AS DAY_NUMBER_IN_WEEK,
    EXTRACT(DAY FROM TIME_ID)::INT AS DAY_NUMBER_IN_MONTH,
    EXTRACT(WEEK FROM TIME_ID)::INT AS CALENDAR_WEEK_NUMBER,
    EXTRACT(WEEK FROM TIME_ID)::INT AS FISCAL_WEEK_NUMBER,
    TIME_ID + INTERVAL '6 DAYS' - INTERVAL '1 DAY' AS WEEK_ENDING_DAY,
    EXTRACT(YEAR FROM TIME_ID)::INT * 1000 + EXTRACT(WEEK FROM TIME_ID)::INT AS WEEK_ENDING_DAY_ID,
    EXTRACT(MONTH FROM TIME_ID)::INT AS CALENDAR_MONTH_NUMBER,
    EXTRACT(MONTH FROM TIME_ID)::INT AS FISCAL_MONTH_NUMBER,
    TO_CHAR(TIME_ID, 'YYYY-MM') AS CALENDAR_MONTH_DESC,
    EXTRACT(YEAR FROM TIME_ID)::INT * 100 + EXTRACT(MONTH FROM TIME_ID)::INT AS CALENDAR_MONTH_ID,
    TO_CHAR(TIME_ID, 'YYYY-MM') AS FISCAL_MONTH_DESC,
    EXTRACT(YEAR FROM TIME_ID)::INT * 100 + EXTRACT(MONTH FROM TIME_ID)::INT AS FISCAL_MONTH_ID,
    EXTRACT(DAY FROM DATE_TRUNC('MONTH', TIME_ID + INTERVAL '1 MONTH') - DATE_TRUNC('MONTH', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_CAL_MONTH,
    EXTRACT(DAY FROM DATE_TRUNC('MONTH', TIME_ID + INTERVAL '1 MONTH') - DATE_TRUNC('MONTH', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_FIS_MONTH,
    DATE_TRUNC('MONTH', TIME_ID + INTERVAL '1 MONTH') - INTERVAL '1 DAY' AS END_OF_CAL_MONTH,
    DATE_TRUNC('MONTH', TIME_ID + INTERVAL '1 MONTH') - INTERVAL '1 DAY' AS END_OF_FIS_MONTH,
    TO_CHAR(TIME_ID, 'MONTH') AS CALENDAR_MONTH_NAME,
    TO_CHAR(TIME_ID, 'MONTH') AS FISCAL_MONTH_NAME,
    TO_CHAR(TIME_ID, 'YYYY-Q') AS CALENDAR_QUARTER_DESC,
    EXTRACT(YEAR FROM TIME_ID)::INT * 10 + CEIL(EXTRACT(MONTH FROM TIME_ID)::INT / 3) AS CALENDAR_QUARTER_ID,
    TO_CHAR(TIME_ID, 'YYYY-Q') AS FISCAL_QUARTER_DESC,
    EXTRACT(YEAR FROM TIME_ID)::INT * 10 + CEIL(EXTRACT(MONTH FROM TIME_ID)::INT / 3) AS FISCAL_QUARTER_ID,
    EXTRACT(DAY FROM DATE_TRUNC('QUARTER', TIME_ID + INTERVAL '3 MONTHS') - DATE_TRUNC('QUARTER', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_CAL_QUARTER,
    EXTRACT(DAY FROM DATE_TRUNC('QUARTER', TIME_ID + INTERVAL '3 MONTHS') - DATE_TRUNC('QUARTER', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_FIS_QUARTER,
    DATE_TRUNC('QUARTER', TIME_ID + INTERVAL '3 MONTHS') - INTERVAL '1 DAY' AS END_OF_CAL_QUARTER,
    DATE_TRUNC('QUARTER', TIME_ID + INTERVAL '3 MONTHS') - INTERVAL '1 DAY' AS END_OF_FIS_QUARTER,
    EXTRACT(QUARTER FROM TIME_ID)::INT AS CALENDAR_QUARTER_NUMBER,
    EXTRACT(QUARTER FROM TIME_ID)::INT AS FISCAL_QUARTER_NUMBER,
    EXTRACT(YEAR FROM TIME_ID)::INT AS CALENDAR_YEAR,
    EXTRACT(YEAR FROM TIME_ID)::INT AS CALENDAR_YEAR_ID,
    EXTRACT(YEAR FROM TIME_ID)::INT AS FISCAL_YEAR,
    EXTRACT(YEAR FROM TIME_ID)::INT AS FISCAL_YEAR_ID,
    EXTRACT(DAY FROM DATE_TRUNC('YEAR', TIME_ID + INTERVAL '1 YEAR') - DATE_TRUNC('YEAR', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_CAL_YEAR,
    EXTRACT(DAY FROM DATE_TRUNC('YEAR', TIME_ID + INTERVAL '1 YEAR') - DATE_TRUNC('YEAR', TIME_ID) + INTERVAL '1 DAY')::INT AS DAYS_IN_FIS_YEAR,
    DATE_TRUNC('YEAR', TIME_ID + INTERVAL '1 YEAR') - INTERVAL '1 DAY' AS END_OF_CAL_YEAR,
    DATE_TRUNC('YEAR', TIME_ID + INTERVAL '1 YEAR') - INTERVAL '1 DAY' AS END_OF_FIS_YEAR
FROM GENERATE_SERIES('1950-01-01'::DATE, '2150-01-01'::DATE, '1 DAY'::INTERVAL) AS TIME_ID;
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


CREATE OR REPLACE PROCEDURE bl_cl.load_dim_payment_methods_table()
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
    message := 'filling DIM_PAYMENT_METHODS table completed';
    schema_name := 'bl_dm';
    table_name := 'DIM_PAYMENT_METHODS';
    start_time := CURRENT_TIMESTAMP;

    INSERT INTO bl_dm.DIM_PAYMENT_METHODS (PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    SELECT
        PAYMENT_METHOD_SURR_ID,
        PAYMENT_METHOD_NAME,
        'bl_3nf',
        'CE_PAYMENT_METHODS',
        INSERT_DT,
        UPDATE_DT
    FROM BL_3NF.CE_PAYMENT_METHODS
	 WHERE BL_3NF.CE_PAYMENT_METHODS.PAYMENT_METHOD_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM bl_dm.DIM_PAYMENT_METHODS WHERE bl_dm.DIM_PAYMENT_METHODS.PAYMENT_METHOD_SURR_ID = BL_3NF.CE_PAYMENT_METHODS.PAYMENT_METHOD_SURR_ID
  )
  ON CONFLICT DO NOTHING;
  

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




CREATE OR REPLACE PROCEDURE bl_cl.load_dim_shipping_methods_table()
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
	message := 'filling DIM_SHIPPING_METHODS table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_SHIPPING_METHODS';
    start_time := CURRENT_TIMESTAMP;


  INSERT INTO bl_dm.DIM_SHIPPING_METHODS (SHIPPING_METHOD_SRC_ID, SHIPPING_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, 
										  INSERT_DT, UPDATE_DT)
  SELECT
    SHIPPING_METHOD_SURR_ID,
    SHIPPING_METHOD_NAME,
    'bl_3nf',
    'CE_SHIPPING_METHOD',
    INSERT_DT,
    UPDATE_DT
  FROM BL_3NF.CE_SHIPPING_METHOD
  WHERE SHIPPING_METHOD_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM bl_dm.DIM_SHIPPING_METHODS WHERE bl_dm.DIM_SHIPPING_METHODS.SHIPPING_METHOD_SURR_ID = BL_3NF.CE_SHIPPING_METHOD.SHIPPING_METHOD_SURR_ID
  )
  ON CONFLICT DO NOTHING;
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

CREATE OR REPLACE PROCEDURE bl_cl.load_dim_online_add_channels_table()
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
	message := 'filling DIM_ONLINE_ADD_CHANNELS table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_ONLINE_ADD_CHANNELS';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO bl_dm.DIM_ONLINE_ADD_CHANNELS (ONLINE_ADD_CHANNEL_SRC_ID, ONLINE_ADD_CHANNEL_NAME, 
											 SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT
    ONLINE_ADD_CHANNEL_SURR_ID,
    ONLINE_ADD_CHANNEL_NAME,
    'bl_3nf',
    'CE_ONLINE_ADD_CHANELS',
    INSERT_DT,
    UPDATE_DT
  FROM BL_3NF.CE_ONLINE_ADD_CHANELS
  WHERE ONLINE_ADD_CHANNEL_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM bl_dm.DIM_ONLINE_ADD_CHANNELS WHERE bl_dm.DIM_ONLINE_ADD_CHANNELS.ONLINE_ADD_CHANNEL_SURR_ID = BL_3NF.CE_ONLINE_ADD_CHANELS.ONLINE_ADD_CHANNEL_SURR_ID
  )
  ON CONFLICT DO NOTHING;
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

CREATE OR REPLACE PROCEDURE bl_cl.load_dim_webs_table()
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
	message := 'filling DIM_WEBS table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_WEBS';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO bl_dm.DIM_WEBS (WEB_SRC_ID, WEB_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT
    WEB_SURR_ID,
    WEB_NAME,
    'bl_3nf',
    'CE_WEBS',
    INSERT_DT,
    UPDATE_DT
  FROM BL_3NF.CE_WEBS
  WHERE WEB_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM bl_dm.DIM_WEBS WHERE bl_dm.DIM_WEBS.WEB_SURR_ID = BL_3NF.CE_WEBS.WEB_SURR_ID
  )
  ON CONFLICT DO NOTHING;
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

CREATE OR REPLACE PROCEDURE bl_cl.load_dim_customers_table()
LANGUAGE plpgsql
AS
$$
DECLARE

    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
	user_names  text;
	message text;
	schema_name text;
	table_name text;
BEGIN
   user_names := current_user;
	message := 'filling DIM_CUSTOMERS table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_CUSTOMERS';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO bl_dm.DIM_CUSTOMERS (CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, USER_NAME, PHONE, EMAIL, 
								   SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT
    CUSTOMER_SURR_ID,
    CUSTOMER_FIRST_NAME,
    CUSTOMER_LAST_NAME,
    USER_NAME,
    PHONE,
    EMAIL,
    'bl_3nf',
    'CE_CUSTOMERS',
    INSERT_DT,
    UPDATE_DT
  FROM BL_3NF.CE_CUSTOMERS
  WHERE CUSTOMER_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM bl_dm.DIM_CUSTOMERS WHERE bl_dm.DIM_CUSTOMERS.CUSTOMER_SURR_ID = BL_3NF.CE_CUSTOMERS.CUSTOMER_SURR_ID
  )
  ON CONFLICT DO NOTHING;
  	end_time := CURRENT_TIMESTAMP;
    run_time := end_time - start_time;

BEGIN 
CALL bl_cl.log_event(user_names, message, schema_name, table_name, start_time, end_time, run_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during loading: %', SQLERRM;
        -- Log the event before re-raising the exception
        CALL bl_cl.log_event(user_name, SQLERRM, schema_name, table_name, start_time, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - start_time);
        RAISE;
END;
END;
$$;



CREATE OR REPLACE PROCEDURE bl_cl.load_dim_stores_table()
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
	message := 'filling DIM_STORES table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_STORES';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO BL_DM.DIM_STORES (STORE_SRC_ID, STORE_NAME, ADDRESS_NAME, PHONE, EMAIL, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT
    s.STORE_SURR_ID,
    s.STORE_NAME,
    a.ADDRESS_NAME,
    s.PHONE,
    s.EMAIL,
    'bl_3nf',
    'CE_STORES',
    s.INSERT_DT,
    s.UPDATE_DT
  FROM BL_3NF.CE_STORES s
  LEFT JOIN BL_3NF.CE_ADDRESSES_SCD2 a on s.ADDRESS_SRC_ID = a.ADDRESS_SRC_ID
  WHERE STORE_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM BL_DM.DIM_STORES WHERE BL_DM.DIM_STORES.STORE_SURR_ID = s.STORE_SURR_ID
  )
  ON CONFLICT DO NOTHING;
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


CREATE OR REPLACE PROCEDURE bl_cl.load_dim_books_table()
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
	message := 'filling DIM_BOOKS table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_BOOKS';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO BL_DM.DIM_BOOKS (BOOK_SRC_ID, BOOK_NAME, GENRE_NAME, AUTHOR_FIRST_NAME, AUTHOR_LAST_NAME, FORMAT_NAME, BOOKS_PRICE, 
							   SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    b.BOOK_SURR_ID,
    b.BOOK_NAME,
    g.GENRE_NAME,
	a.AUTHOR_FIRST_NAME,
    a.AUTHOR_LAST_NAME,
	f.FORMAT_NAME,
    b.BOOKS_PRICE,
    'bl_3nf',
    'CE_BOOKS',
    b.INSERT_DT,
    b.UPDATE_DT
   FROM BL_3NF.CE_BOOKS b
  left JOIN (select distinct GENRE_SRC_ID, GENRE_NAME from BL_3NF.CE_GENRES) g on b.GENRE_SRC_ID = g.GENRE_SRC_ID
  left JOIN (select distinct AUTHOR_SRC_ID, AUTHOR_FIRST_NAME,AUTHOR_LAST_NAME from BL_3NF.CE_AUTHORS) a on b.AUTHOR_SRC_ID = a.AUTHOR_SRC_ID
  left JOIN (select distinct FORMAT_SRC_ID, upper(FORMAT_NAME) as FORMAT_NAME from  BL_3NF.CE_FORMATS) f on b.FORMAT_SRC_ID = f.FORMAT_SRC_ID
 WHERE BOOK_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM BL_DM.DIM_BOOKS WHERE BL_DM.DIM_BOOKS.BOOK_SURR_ID = b.BOOK_SURR_ID
  )
  ON CONFLICT DO NOTHING;
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

CREATE OR REPLACE PROCEDURE bl_cl.initial_load_dim_addresses_scd2()
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
	message := 'filling DIM_ADDRESSES_SCD2 table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_ADDRESSES_SCD2';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO BL_DM.DIM_ADDRESSES_SCD2 (ADDRESS_SRC_ID, ADDRESS_NAME, STREET_NAME, CITY_NAME, COUNTRY_NAME, COUNTRY_CODE, 
										SOURCE_SYSTEM, SOURCE_ENTITY, START_DT, END_DT, IS_ACTIVE, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    a.ADDRESS_SURR_ID,
    a.ADDRESS_NAME,
    s.STREET_NAME,
    c.CITY_NAME,
    co.COUNTRY_NAME,
    co.COUNTRY_CODE,
    'bl_3nf',
    'CE_ADDRESSES_SCD2',
    START_DT,
    END_DT,
    IS_ACTIVE,
    a.INSERT_DT,
    a.UPDATE_DT
  FROM BL_3NF.CE_ADDRESSES_SCD2 a
LEFT JOIN BL_3NF.CE_COUNTRIES co on a.COUNTRY_SRC_ID = co.COUNTRY_SRC_ID  
LEFT JOIN BL_3NF.CE_CITIES c on a.CITY_SRC_ID = c.CITY_SRC_ID
LEFT JOIN BL_3NF.CE_STREETS s on a.STREET_SRC_ID = s.STREET_SRC_ID
  WHERE ADDRESS_SURR_ID > 0 AND not EXISTS (
    SELECT 1 FROM BL_DM.DIM_ADDRESSES_SCD2 WHERE BL_DM.DIM_ADDRESSES_SCD2.ADDRESS_SURR_ID = a.ADDRESS_SURR_ID
  )
  ON CONFLICT DO NOTHING;
  
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


CREATE OR REPLACE PROCEDURE bl_cl.load_dim_addresses_scd2()
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
	message := 'filling DIM_ADDRESSES_SCD2 table completed';
	schema_name := 'bl_dm';
	table_name := 'DIM_ADDRESSES_SCD2';
    start_time := CURRENT_TIMESTAMP;

  -- Update existing records with changes
  UPDATE BL_DM.DIM_ADDRESSES_SCD2 AS d
  SET END_DT = current_date,  -- Set the end date to the current date for the old record
      IS_ACTIVE = 'N',          -- Mark the old record as inactive
	  update_dt = current_date
  FROM BL_3NF.CE_ADDRESSES_SCD2 a
  LEFT JOIN BL_3NF.CE_COUNTRIES co ON a.COUNTRY_SRC_ID = co.COUNTRY_SRC_ID  
  LEFT JOIN BL_3NF.CE_CITIES c ON a.CITY_SRC_ID = c.CITY_SRC_ID
  LEFT JOIN BL_3NF.CE_STREETS s ON a.STREET_SRC_ID = s.STREET_SRC_ID
  WHERE a.ADDRESS_SURR_ID = d.ADDRESS_SURR_ID
    AND (
      
      co.COUNTRY_NAME <> d.COUNTRY_NAME OR
      co.COUNTRY_CODE <> d.COUNTRY_CODE OR
      c.CITY_NAME <> d.CITY_NAME OR
      s.STREET_NAME <> d.STREET_NAME OR
      a.ADDRESS_NAME <> d.ADDRESS_NAME
    );

  -- Insert new records for changed data
  INSERT INTO BL_DM.DIM_ADDRESSES_SCD2 (ADDRESS_SRC_ID, ADDRESS_NAME, STREET_NAME, CITY_NAME, COUNTRY_NAME, COUNTRY_CODE, 
                                        SOURCE_SYSTEM, SOURCE_ENTITY, START_DT, END_DT, IS_ACTIVE, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    a.ADDRESS_SURR_ID,
    a.ADDRESS_NAME,
    s.STREET_NAME,
    c.CITY_NAME,
    co.COUNTRY_NAME,
    co.COUNTRY_CODE,
    'bl_3nf',
    'CE_ADDRESSES_SCD2',
    a.START_DT,
    '9999-12-31'::DATE,  
    1::BOOL,             
    a.INSERT_DT,
    a.UPDATE_DT
  FROM BL_3NF.CE_ADDRESSES_SCD2 a
  left JOIN (select distinct COUNTRY_SRC_ID, COUNTRY_NAME, COUNTRY_CODE from  BL_3NF.CE_COUNTRIES) co ON a.COUNTRY_SRC_ID = co.COUNTRY_SRC_ID  
  LEFT JOIN BL_3NF.CE_CITIES c ON a.CITY_SRC_ID = c.CITY_SRC_ID
  LEFT JOIN BL_3NF.CE_STREETS s ON a.STREET_SRC_ID = s.STREET_SRC_ID
  WHERE a.ADDRESS_SURR_ID > 0   AND EXISTS (
    SELECT 1 FROM BL_DM.DIM_ADDRESSES_SCD2 dd
    WHERE dd.ADDRESS_SURR_ID = a.ADDRESS_SURR_ID
    AND dd.END_DT = '9999-12-31'::DATE
  );
  
  
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

