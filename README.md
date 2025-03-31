 This Snowflake SQL procedure, `LoadAccountsToAzureContainer`, is designed to export inactive customer accounts to Azure Blob Storage in JSON format. It is a structured and automated process that handles data preparation, batching, export, and logging, ensuring efficient and reliable data transfer.

The procedure begins by declaring variables to store key configuration details, such as database and table names, batch size, and the external stage location for storing the exported files. A `v_TestRun` flag is included to limit the number of records processed during development or testing, ensuring safe execution in non-production environments.

The procedure first checks if it is running in a development environment by inspecting the database name. If the database name starts with "DEV_", it adjusts the variable values to point to development-specific tables and enforces test mode. In test mode, only 10 customer records are retained for export, preventing excessive data processing during testing.

Next, the procedure calculates a `File_Group` for batching customer accounts into groups of up to `v_BatchSize` records (defaulting to 10,000). This grouping is achieved using the `ROW_NUMBER` function and the `CEIL` function. The grouped records are updated in the source table to assign the calculated `File_Group` values.

The procedure dynamically constructs and executes a query to retrieve distinct `File_Group` values for unsent records. It iterates through these groups using a `FOR` loop, exporting each group to a JSON file in Azure Blob Storage. The `COPY INTO` command is used to generate the JSON files, with each file containing aggregated customer data structured using the `ARRAY_AGG` and `OBJECT_CONSTRUCT` functions. The file size is limited to 50 MB to ensure manageability.

After exporting the data, the procedure updates the master customer table to increment the count of how many times each account has been sent to Azure. It also marks the exported records in the source table as sent and updates their modification timestamps.

Finally, the procedure returns a success message indicating the number of customer accounts exported. If no rows were exported, it returns a message stating that no rows were sent. In case of an error, the procedure logs the error details using an audit logging mechanism and re-raises the exception to ensure proper error handling.

This procedure is robust and well-structured, with clear handling for development and production environments, efficient batching of data, and comprehensive error logging. However, the `v_TestRun` flag includes a TODO comment to disable test mode in production, which should be addressed before deployment.
