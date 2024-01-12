
CREATE OR REPLACE PROCEDURE bl_cl.creating_tables_cl_layer()

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
	message := 'creating all tables cl layer completed';
	schema_name := 'bl_cl';
	table_name := 'all_tables';
    start_time := CURRENT_TIMESTAMP;

CREATE TABLE if not exists bl_cl.PRM_MTA_INCREMENTAL_LOAD (
    source_table_name TEXT,
    target_table_name TEXT,
    procedure_name TEXT,
    previous_loaded_date TIMESTAMP
);
INSERT INTO bl_cl.PRM_MTA_INCREMENTAL_LOAD (source_table_name, target_table_name, procedure_name, previous_loaded_date)
VALUES ('MANUAL', 'MANUAL', 'MANUAL', '1900-01-01');
      
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

call bl_cl.creating_tables_cl_layer()