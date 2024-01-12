

CREATE OR REPLACE PROCEDURE bl_cl.load_CE_ADDRESSES_table()
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


 UPDATE BL_3NF.CE_ADDRESSES_SCD2 AS d
  SET END_DT = current_date,  -- Set the end date to the current date for the old record
      IS_ACTIVE = 'N',          -- Mark the old record as inactive
	  update_dt = current_date
  FROM sa_offline_sales.src_offline_sales a
  
  WHERE a.address_id = d.ADDRESS_SRC_ID
    AND   a.ADDRESS_NAME <> d.ADDRESS_NAME
	and IS_ACTIVE = 'y'
	;
		

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
FROM sa_offline_sales.src_offline_sales a
WHERE a.address_id > 0 AND NOT EXISTS (
    SELECT 1 FROM BL_3NF.CE_ADDRESSES_SCD2 dd
    WHERE dd.ADDRESS_SRC_ID >0 and  dd.ADDRESS_SRC_ID = a.address_id
    AND dd.END_DT = '2099-12-31'::DATE
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


call bl_cl.load_CE_ADDRESSES_table()
