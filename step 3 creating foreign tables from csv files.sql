
CREATE OR REPLACE PROCEDURE bl_cl.loading_sa_offline_sales()
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
	message := 'loading src_offline_sales completed.';
	schema_name := 'sa_offline_sales';
	table_name := 'src_offline_sales';
    start_time := CURRENT_TIMESTAMP;

drop FOREIGN table IF EXISTS  sa_offline_sales.src_offline_sales;

CREATE FOREIGN TABLE IF NOT EXISTS  sa_offline_sales.src_offline_sales (
    
    SALE_DATE date,
    STORE_ID int,
    store_name TEXT,	
    address_id int,
    store_phone  TEXT,	
    store_email  TEXT,
    address_name  TEXT,
    street_id	int,
    street_name  TEXT,
    city_id	int,
    country_id	int,
    city_name  TEXT,
    country_name  TEXT,
    country_code   TEXT,
    book_id 	int,
    book_name   TEXT,
    genre_id	int,
    author_id	int,
    format_id	int,
    books_price  TEXT,
    AMOUNT	int,
    genre_name   TEXT,
    author_first_name   TEXT,
    author_last_name   TEXT,
    BOOK_FORMAT TEXT
)
SERVER sa_sales_server
OPTIONS (
  format 'csv',
  filename 'D:\Liza\ext_offline_sales.csv',
  delimiter ';',
  header 'true'
);

    end_time := CURRENT_TIMESTAMP;
    run_time := end_time - start_time;
BEGIN 
CALL bl_cl.log_event(user_name, message, schema_name, table_name, start_time, end_time, run_time);

EXCEPTION
    WHEN OTHERS THEN
        
        RAISE NOTICE 'Error occurred during loading: %', SQLERRM;
        ROLLBACK;
END;
END;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.loading_sa_online_sales()
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
	message := 'loading src_online_sales completed.';
	schema_name := 'sa_online_sales';
	table_name := 'src_online_sales';
    start_time := CURRENT_TIMESTAMP;

drop FOREIGN table IF EXISTS  sa_online_sales.src_online_sales;

CREATE FOREIGN TABLE IF NOT EXISTS  sa_online_sales.src_online_sales (
    
    SALE_DATE date,
    country_id	int,
    country_name  TEXT,
    country_code   TEXT,
    book_id 	int,
    book_name   TEXT,
    genre_id	int,
    author_id	int,
    format_id	int,
    books_price  TEXT,
    AMOUNT	int,
    customer_id		int,
    customer_first_name	   TEXT,
    customer_last_name	   TEXT,
    user_name	   TEXT,
    customer_phone   TEXT,
    customer_email   TEXT,
    genre_name   TEXT,
    author_first_name   TEXT,
    author_last_name   TEXT,
    BOOK_FORMAT TEXT,
    PAYMENT_METHOD_ID	int,
    PAYMENT_METHOD_name TEXT,
    online_add_channel_id	int,
    online_add_channel_name TEXT,
    Shipping_Method_id	int,
    Shipping_Method_name TEXT,
    web_name	TEXT,
    web_id	int	
)
SERVER sa_sales_server
OPTIONS (
  format 'csv',
  filename 'D:\Liza\ext_online_sales.csv',
  delimiter ';',
  header 'true'
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



call bl_cl.loading_sa_offline_sales()
call bl_cl.loading_sa_online_sales()