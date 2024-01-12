CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_PAYMENT_METHODS_table()
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
	message := 'filling CE_PAYMENT_METHODS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_PAYMENT_METHODS';
    start_time := CURRENT_TIMESTAMP;
 
  INSERT INTO bl_3nf.CE_PAYMENT_METHODS (PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    PAYMENT_METHOD_ID,
    PAYMENT_METHOD_name,
    'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE PAYMENT_METHOD_ID > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_PAYMENT_METHODS  WHERE BL_3NF.CE_PAYMENT_METHODS.PAYMENT_METHOD_SRC_ID = sa_online_sales.src_online_sales.PAYMENT_METHOD_ID
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



CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_ONLINE_ADD_CHANELS_table()
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
	message := 'filling CE_ONLINE_ADD_CHANELS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_ONLINE_ADD_CHANELS';
    start_time := CURRENT_TIMESTAMP;
 
  INSERT INTO BL_3NF.CE_ONLINE_ADD_CHANELS (ONLINE_ADD_CHANNEL_SRC_ID, ONLINE_ADD_CHANNEL_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    online_add_channel_id,
    online_add_channel_name,
    'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE online_add_channel_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_ONLINE_ADD_CHANELS  WHERE BL_3NF.CE_ONLINE_ADD_CHANELS.ONLINE_ADD_CHANNEL_SRC_ID = sa_online_sales.src_online_sales.online_add_channel_id
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

CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_SHIPPING_METHOD_table()
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
	message := 'filling CE_SHIPPING_METHOD table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_SHIPPING_METHOD';
    start_time := CURRENT_TIMESTAMP;
 

  INSERT INTO BL_3NF.CE_SHIPPING_METHOD (SHIPPING_METHOD_SRC_ID, SHIPPING_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    Shipping_Method_id,
    Shipping_Method_name,
    'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE Shipping_Method_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_SHIPPING_METHOD  WHERE BL_3NF.CE_SHIPPING_METHOD.SHIPPING_METHOD_SRC_ID = sa_online_sales.src_online_sales.Shipping_Method_id
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
  
  
CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_WEBS_table()
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
	message := 'filling CE_WEBS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_WEBS';
    start_time := CURRENT_TIMESTAMP;

  INSERT INTO BL_3NF.CE_WEBS (WEB_SRC_ID, WEB_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    web_id,
    web_name,
    'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE web_id > 0  and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_WEBS  WHERE BL_3NF.CE_WEBS.WEB_SRC_ID = sa_online_sales.src_online_sales.web_id
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


CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_CUSTOMERS_table()
LANGUAGE plpgsql
AS
$$
DECLARE
DECLARE

    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
	pstg_user_name  text;
	message text;
	schema_name text;
	table_name text;
BEGIN
   pstg_user_name := current_user;
	message := 'filling CE_CUSTOMERS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_CUSTOMERS';
    start_time := CURRENT_TIMESTAMP;


  INSERT INTO BL_3NF.CE_CUSTOMERS (CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, USER_NAME, PHONE,  EMAIL,  SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    customer_id,
    customer_first_name,
	customer_last_name, 
	user_name, 
	customer_phone, 
	customer_email,
    'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE customer_id > 0  and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_CUSTOMERS  WHERE BL_3NF.CE_CUSTOMERS.CUSTOMER_SRC_ID = sa_online_sales.src_online_sales.customer_id
) 
  ON CONFLICT DO NOTHING;
    end_time := CURRENT_TIMESTAMP;
    run_time := end_time - start_time;

BEGIN 
CALL bl_cl.log_event(pstg_user_name, message, schema_name, table_name, start_time, end_time, run_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during loading: %', SQLERRM;
        -- Log the event before re-raising the exception
        CALL bl_cl.log_event(user_name, SQLERRM, schema_name, table_name, start_time, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - start_time);
        RAISE;
END;  
END;
$$;
  
CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_FORMATS_table()
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
	message := 'filling CE_FORMATS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_FORMATS';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_FORMATS (FORMAT_SRC_ID, FORMAT_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
SELECT DISTINCT
    format_id,
    BOOK_FORMAT,
	'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE format_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_FORMATS  WHERE BL_3NF.CE_FORMATS.FORMAT_SRC_ID = sa_online_sales.src_online_sales.format_id 
	 
) 

UNION ALL
SELECT DISTINCT
    format_id,
    BOOK_FORMAT,
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE format_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_FORMATS  WHERE BL_3NF.CE_FORMATS.FORMAT_SRC_ID = sa_offline_sales.src_offline_sales.format_id
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
 
CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_COUNTRIES_table()
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
	message := 'filling CE_COUNTRIES table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_COUNTRIES';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_COUNTRIES (COUNTRY_SRC_ID, COUNTRY_NAME, COUNTRY_CODE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    country_id,
    country_name,
	country_code, 
	'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE country_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_COUNTRIES  WHERE BL_3NF.CE_COUNTRIES.COUNTRY_SRC_ID = sa_online_sales.src_online_sales.country_id
	 
) 
UNION ALL   
  SELECT DISTINCT
    country_id,
    country_name,
	country_code, 
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE country_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_COUNTRIES  WHERE BL_3NF.CE_COUNTRIES.COUNTRY_SRC_ID = sa_offline_sales.src_offline_sales.country_id)
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

CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_AUTHORS_table()
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
	message := 'filling CE_AUTHORS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_AUTHORS';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_AUTHORS (AUTHOR_SRC_ID, AUTHOR_FIRST_NAME, AUTHOR_LAST_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    author_id,
    author_first_name,
	author_last_name, 
	'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE author_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_AUTHORS  WHERE BL_3NF.CE_AUTHORS.AUTHOR_SRC_ID = sa_online_sales.src_online_sales.author_id
	  
) 
UNION ALL     
  SELECT DISTINCT
    author_id,
    author_first_name,
	author_last_name, 
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE author_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_AUTHORS  WHERE BL_3NF.CE_AUTHORS.AUTHOR_SRC_ID = sa_offline_sales.src_offline_sales.author_id
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



CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_GENRES_table()
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
	message := 'filling CE_GENRES table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_GENRES';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_GENRES (GENRE_SRC_ID, GENRE_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    genre_id,
    genre_name,
	'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE genre_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_GENRES  WHERE BL_3NF.CE_GENRES.GENRE_SRC_ID = sa_online_sales.src_online_sales.genre_id
	 
) 
UNION ALL     
  SELECT DISTINCT
   genre_id,
    genre_name,
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE genre_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_GENRES  WHERE BL_3NF.CE_GENRES.GENRE_SRC_ID = sa_offline_sales.src_offline_sales.genre_id
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


CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_BOOKS_table()
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
	message := 'filling CE_BOOKS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_BOOKS';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_BOOKS (BOOK_SRC_ID, BOOK_NAME,  GENRE_SRC_ID ,  AUTHOR_SRC_ID ,  FORMAT_SRC_ID , BOOKS_PRICE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    book_id,
	book_name,
	genre_id,
	author_id,
	format_id,
	 REPLACE(books_price, ',', '.')::NUMERIC,
	'sa_online_sales',
    'src_online_sales',
    current_date,
    current_date
  FROM sa_online_sales.src_online_sales
  WHERE book_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_BOOKS  WHERE BL_3NF.CE_BOOKS.BOOK_SRC_ID = sa_online_sales.src_online_sales.book_id
) 
UNION ALL
SELECT DISTINCT
    book_id,
	book_name,
	genre_id,
	author_id,
	format_id,
	 REPLACE(books_price, ',', '.')::NUMERIC,
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE book_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_BOOKS  WHERE BL_3NF.CE_BOOKS.BOOK_SRC_ID = sa_offline_sales.src_offline_sales.book_id
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


CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_STORES_table()
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
	message := 'filling CE_STORES table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_STORES';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_STORES (STORE_SRC_ID, STORE_NAME, ADDRESS_SRC_ID,PHONE ,  EMAIL,							  
							  SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    STORE_ID,
    store_name,
	address_id, 
	store_phone,
	store_email,
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE STORE_ID > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_STORES  WHERE BL_3NF.CE_STORES.STORE_SRC_ID = sa_offline_sales.src_offline_sales.STORE_ID
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



CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_CITIES_table()
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
	message := 'filling CE_CITIES table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_CITIES';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_CITIES (CITY_SRC_ID, CITY_NAME, COUNTRY_SRC_ID, 						  
							  SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    city_id,
    city_name,
	country_id, 
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE city_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_CITIES  WHERE BL_3NF.CE_CITIES.CITY_SRC_ID = sa_offline_sales.src_offline_sales.city_id
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


CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_STREETS_table()
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
	message := 'filling CE_STREETS table completed';
	schema_name := 'bl_3nf';
	table_name := 'CE_STREETS';
    start_time := CURRENT_TIMESTAMP;
INSERT INTO BL_3NF.CE_STREETS (STREET_SRC_ID, STREET_NAME, CITY_SRC_ID, 						  
							  SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
  SELECT DISTINCT
    street_id,
    street_name,
	city_id, 
	'sa_offline_sales',
    'src_offline_sales',
    current_date,
    current_date
  FROM sa_offline_sales.src_offline_sales
  WHERE street_id > 0 and NOT EXISTS (
  SELECT 1 FROM BL_3NF.CE_STREETS  WHERE BL_3NF.CE_STREETS.STREET_SRC_ID = sa_offline_sales.src_offline_sales.street_id
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



CREATE OR REPLACE PROCEDURE bl_cl.initial_load_CE_ADDRESSES_table()
LANGUAGE plpgsql
AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
    user_name TEXT;
    message TEXT;
    schema_name TEXT;
    table_name TEXT;
BEGIN
    user_name := current_user;
    message := 'Filling CE_ADDRESSES_SCD2 table completed';
    schema_name := 'bl_3nf';
    table_name := 'CE_ADDRESSES_SCD2';
    start_time := CURRENT_TIMESTAMP;

    INSERT INTO BL_3NF.CE_ADDRESSES_SCD2 (ADDRESS_SRC_ID, ADDRESS_NAME, STREET_SRC_ID, CITY_SRC_ID, COUNTRY_SRC_ID, 						  
                                         SOURCE_SYSTEM, SOURCE_ENTITY, START_DT, END_DT, IS_ACTIVE, INSERT_DT, UPDATE_DT)
    SELECT DISTINCT
        address_id,
        address_name,
        street_id, 
        city_id,
        country_id,
        'sa_offline_sales',
        'src_offline_sales',
        current_date,
        '2099-12-31'::date,
        1:: bool,
        current_date,
        current_date
    FROM sa_offline_sales.src_offline_sales
    WHERE address_id > 0 AND NOT EXISTS (
        SELECT 1
        FROM BL_3NF.CE_ADDRESSES_SCD2
        WHERE BL_3NF.CE_ADDRESSES_SCD2.ADDRESS_SRC_ID = sa_offline_sales.src_offline_sales.address_id
        AND BL_3NF.CE_ADDRESSES_SCD2.IS_ACTIVE = 'Y'
    );

  
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
