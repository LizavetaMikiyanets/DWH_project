CREATE TABLE IF NOT EXISTS bl_cl.logging_table (
    log_id SERIAL PRIMARY KEY,
    schema_name TEXT,
    table_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    run_time INTERVAL,
    log_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    log_user TEXT NOT NULL,
    log_message TEXT NOT NULL
);


CREATE OR REPLACE PROCEDURE bl_cl.log_event(
    user_name TEXT,
    message TEXT,
	schema_name TEXT,
    table_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    run_time INTERVAL
	)
LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO bl_cl.logging_table (log_user, log_message, schema_name, table_name,start_time, end_time,run_time ) 
								VALUES (user_name, message, schema_name, table_name,start_time, end_time,run_time);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during logging: %', SQLERRM;
        ROLLBACK;
END;
$$;

