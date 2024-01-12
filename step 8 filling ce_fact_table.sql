CREATE OR REPLACE PROCEDURE bl_cl.PARTITIONING_CE_SALE(rolling_window_months INTEGER)
LANGUAGE plpgsql
AS
$$
DECLARE 
    rolling_window_start_date DATE := CURRENT_DATE - (rolling_window_months || ' months')::INTERVAL;
    rolling_window_end_date DATE := CURRENT_DATE;
    new_partition_date DATE;
    detached_table_name TEXT;
	 partition_key TEXT;
    partition_name TEXT;
    partition_exists BOOLEAN; 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
	user_name  text;
	message text;
	schema_name text;
	tables_name text;
BEGIN
   user_name := current_user;
	message := 'partitioning bl_3nf.CE_SALE table completed';
	schema_name := 'bl_3nf';
	tables_name := 'CE_SALE';
    start_time := CURRENT_TIMESTAMP;


   
    -- Detach partitions older than the rolling window
    FOR detached_table_name IN (
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'ce_sales_%' 
          AND table_name != 'ce_sales_default'
          AND table_name < 'ce_sales_' || to_char(rolling_window_start_date, 'YYYY_MM')
    )
    LOOP
        -- Check if the table (partition) exists before detaching it
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ''' || detached_table_name || ''' AND table_schema = ''bl_3nf'')'
        INTO partition_exists;

        IF partition_exists THEN
           partition_key := SUBSTRING(detached_table_name, LENGTH('ce_sales_') + 1);
            EXECUTE 'ALTER TABLE bl_3nf.ce_sales DETACH PARTITION ' || 'ce_sales_' || partition_key;
        END IF;
    END LOOP;

    -- Attach new partitions for the current month and future months within the rolling window
    FOR new_partition_date IN (
        SELECT generate_series(rolling_window_start_date, rolling_window_end_date, '1 month'::INTERVAL)::DATE
    )
    LOOP
        -- Generate partition name based on the date in the format "ce_sales_yyyymm"
        partition_name := 'ce_sales_' || TO_CHAR(new_partition_date, 'YYYY_MM');

        -- Check if the table (partition) exists before creating it
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ''' || 'bl_3nf.' || partition_name || ''' AND table_schema = ''bl_3nf'')'
        INTO partition_exists;

        IF NOT partition_exists THEN
            EXECUTE 'CREATE TABLE IF NOT EXISTS bl_3nf.' || partition_name || ' PARTITION OF bl_3nf.ce_sales FOR VALUES FROM (' || quote_literal(new_partition_date) || ') TO (' || quote_literal(new_partition_date + INTERVAL '1 month') || ')';
     END IF;
    END LOOP;
  	end_time := CURRENT_TIMESTAMP;
    run_time := end_time - start_time;

BEGIN 
CALL bl_cl.log_event(user_name, message, schema_name, tables_name, start_time, end_time, run_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during loading: %', SQLERRM;
        -- Log the event before re-raising the exception
        CALL bl_cl.log_event(user_name, SQLERRM, schema_name, table_name, start_time, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP - start_time);
        RAISE;
END;
END;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.load_CE_SALE()
LANGUAGE plpgsql
AS
$$
DECLARE 
    source_table_name_offline TEXT;  
	source_table_name_online TEXT;
    target_table_name TEXT;
    procedure_name TEXT;
	last_date_offline TIMESTAMP;
	last_date_online TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    run_time INTERVAL;
	user_name  text;
	message text;
	schema_name text;
	table_name text;
BEGIN
   user_name := current_user;
	message := 'filling ce_sales table completed';
	schema_name := 'bl_3nf';
	table_name := 'ce_sales';
    start_time := CURRENT_TIMESTAMP;

    source_table_name_offline := 'src_offline_sales';
	source_table_name_online := 'src_online_sales';
    target_table_name := 'ce_sales';
    procedure_name := 'load_CE_SALE';

-- Create the temporary view for incremental sales data
    CREATE TEMP VIEW incr_sales_offline AS
    SELECT *
    FROM sa_offline_sales.src_offline_sales
    WHERE src_offline_sales.sale_date > COALESCE(
        (
            SELECT previous_loaded_date
            FROM bl_cl.PRM_MTA_INCREMENTAL_LOAD
            WHERE source_table_name = 'src_offline_sales'
            ORDER BY previous_loaded_date DESC
            LIMIT 1
        ),
        '2019-01-01' 
    );

	CREATE TEMP VIEW incr_sales_online AS
    SELECT *
    FROM sa_online_sales.src_online_sales
    WHERE src_online_sales.sale_date > COALESCE(
        (
            SELECT previous_loaded_date
            FROM bl_cl.PRM_MTA_INCREMENTAL_LOAD
            WHERE source_table_name = 'src_online_sales'
            ORDER BY previous_loaded_date DESC
            LIMIT 1
        ),
        '2019-01-01' 
    );

    INSERT INTO bl_3nf.ce_sales (TIME_ID, STORE_SRC_ID, ADDRESS_SRC_ID,BOOK_SRC_ID,GENRE_SRC_ID,AUTHOR_SRC_ID,FORMAT_SRC_ID,
    							BOOKS_PRICE,AMOUNT,TOTAL_SALES_AMOUNT,COUNTRY_SRC_ID,CUSTOMER_SRC_ID,PAYMENT_METHOD_SRC_ID,
    							ONLINE_ADD_CHANNEL_SRC_ID, SHIPPING_METHOD_SRC_ID, WEB_SRC_ID )
    SELECT SALE_DATE,
           STORE_ID,
           address_id,
           book_id,
           genre_id,
           author_id,
           format_id,
           REPLACE(books_price, ',', '.')::NUMERIC as books_price,
           AMOUNT,
           REPLACE(books_price, ',', '.')::NUMERIC * AMOUNT as TOTAL_SALES_AMOUNT,
           NULL as country_id,
           NULL as customer_id,
           NULL as PAYMENT_METHOD_ID,
           NULL as online_add_channel_id,
           NULL as Shipping_Method_id,
           NULL as web_id
    FROM incr_sales_offline;
	
	last_date_offline := (select SALE_DATE from sa_offline_sales.src_offline_sales ORDER BY SALE_DATE desc LIMIT 1);
	
    INSERT INTO bl_3nf.ce_sales (TIME_ID, STORE_SRC_ID, ADDRESS_SRC_ID,BOOK_SRC_ID,GENRE_SRC_ID,AUTHOR_SRC_ID,FORMAT_SRC_ID,
    							BOOKS_PRICE,AMOUNT,TOTAL_SALES_AMOUNT,COUNTRY_SRC_ID,CUSTOMER_SRC_ID,PAYMENT_METHOD_SRC_ID,
    							ONLINE_ADD_CHANNEL_SRC_ID, SHIPPING_METHOD_SRC_ID, WEB_SRC_ID )
    SELECT         
		SALE_DATE,
        NULL as STORE_ID,
        NULL as address_id,
        book_id,
        genre_id,
        author_id,
        format_id,
        REPLACE(books_price, ',', '.')::NUMERIC as books_price,
        AMOUNT,
        REPLACE(books_price, ',', '.')::NUMERIC * AMOUNT as TOTAL_SALES_AMOUNT,
        country_id,
        customer_id,
        PAYMENT_METHOD_ID,
        online_add_channel_id,
        Shipping_Method_id,
        web_id

    FROM incr_sales_online;
    -- Drop the temporary view
    DROP VIEW incr_sales_offline;
	DROP VIEW incr_sales_online;
	last_date_online := (select SALE_DATE from sa_online_sales.src_online_sales ORDER BY SALE_DATE desc LIMIT 1);
    -- Update the previous_loaded_date for the source_table_name 'offline_sales'
    INSERT INTO bl_cl.PRM_MTA_INCREMENTAL_LOAD
    VALUES (source_table_name_offline, target_table_name, procedure_name, last_date_offline),
    	   (source_table_name_online, target_table_name, procedure_name, last_date_online);	
	

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


