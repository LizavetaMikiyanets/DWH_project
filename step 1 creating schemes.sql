CREATE SCHEMA IF NOT EXISTS bl_cl;
CREATE OR REPLACE PROCEDURE bl_cl.create_sales_server_and_mapping()
LANGUAGE plpgsql
AS
$$
BEGIN

    CREATE EXTENSION IF NOT EXISTS file_fdw;


    CREATE SERVER IF NOT EXISTS sa_sales_server
    FOREIGN DATA WRAPPER file_fdw;


    CREATE USER MAPPING IF NOT EXISTS FOR current_user
    SERVER sa_sales_server;
	
	
END;
$$;
call bl_cl.create_sales_server_and_mapping()

CREATE OR REPLACE PROCEDURE bl_cl.create_sales_schemas()
LANGUAGE plpgsql
AS
$$
BEGIN
   
    CREATE SCHEMA IF NOT EXISTS sa_offline_sales;
	CREATE SCHEMA IF NOT EXISTS sa_online_sales;
	CREATE SCHEMA IF NOT EXISTS bl_3nf;
    CREATE SCHEMA IF NOT EXISTS bl_dm;
END;
$$;	
call bl_cl.create_sales_schemas()