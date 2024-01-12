# DWH_project
creating a Data Warehouse
Step 1: Creating Schemas
File: step 1 creating schemes.sql
Description: This step involves creating the initial database schemas. Schemas are used to organize and group related tables, views, and other database objects. This is a foundational step in setting up a data warehouse, ensuring that objects are logically separated and managed.
Step 2: Creating Log Table
File: step 2 creating log table.sql
Description: This file focuses on creating a log table. Log tables are essential for tracking events, errors, or changes that occur within the data warehouse. This helps in monitoring and debugging the data loading and transformation processes.
Step 3: Creating Foreign Tables from CSV Files
File: step 3 creating foreign tables from csv files.sql
Description: This step is about creating foreign tables linked to CSV files. This approach is often used for integrating external data sources into a data warehouse, allowing for the direct querying of CSV data as if it were part of the database.
Step 4: Creating Tables in BL_CL Layer (Technical Layer)
File: step 4 creating tables bl_cl layer (technical layer).sql
Description: The BL_CL layer, likely representing a Business Logic or Cleansing Layer, involves creating tables designed for data cleansing and initial processing. This layer serves as an intermediary stage, preparing data for more complex transformations.
Step 5: Creating Tables in BL_3NF Layer
File: step 5 creating tables bl_3nf layer.sql
Description: This step involves creating tables in a BL_3NF (Business Logic Third Normal Form) layer. The 3NF is a database schema design approach for reducing data duplication and ensuring referential integrity, making it ideal for a normalized data layer in a warehouse.
Step 6: Adding Default Values to 3NF Layer
File: step 6 adding default values 3nf layer.sql
Description: This file's focus is on adding default values to the tables in the 3NF layer. Default values can ensure consistency, especially in cases where data may be missing or incomplete during the loading process.
Step 7: Filling the 3NF Layer
File: step 7 filling 3nf layer.sql
Description: In this step, data is loaded into the 3NF layer tables. This could involve importing data from the earlier cleansing layer or directly from external sources, transforming it to fit the normalized structure of the 3NF layer.
Step 8: Filling the CE Fact Table
File: step 8 filling ce_fact_table.sql
Description: This step deals with populating a central fact table, often a key component in a star or snowflake schema in data warehousing. Fact tables typically store quantitative information for analysis and reporting.
Step 9: Creating Tables in DM Layer
File: step 9 creating tables dm layer.sql
Description: The DM (Data Mart) layer involves creating tables specifically designed for data marts. Data marts are subsets of the data warehouse, tailored for specific business lines or reporting requirements.
Step 10: Adding Default Values to DM Layer
File: step 10 adding default values dm layer.sql
Description: Similar to step 6, this step involves adding default values to the tables in the Data Mart layer. This ensures data integrity and consistency in this subset of the data warehouse.
Step 11: Filling the DM Layer
File: step 11 filling dm layer.sql
Description: This file is about loading data into the Data Mart layer tables. This process often involves aggregating, summarizing, or transforming data to suit specific analytical or reporting needs.
Step 12: Updating SCD2 Table
File: step 12 updating SCD2 table.sql
Description: This step is concerned with updating a Slowly Changing Dimension (SCD) type 2 table. SCD2 tables are crucial for tracking historical changes in dimension data, allowing the warehouse to maintain a full history of dimensional changes over time.
