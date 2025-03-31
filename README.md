The Snowflake SQL procedure `LoadAccountsToAzureContainer` automates the export of inactive customer accounts to Azure Blob Storage in JSON format. It streamlines the processes of data preparation, batching, export, and logging, ensuring a secure and efficient transfer of data.

The procedure starts by defining variables that encapsulate essential configuration details, such as the database and table names, batch size, and the external stage location where files will be stored. To facilitate safe testing and development, a `v_TestRun` flag limits the number of records processed during non-production runs.

When executed, the procedure first verifies whether it is running in a development environment by checking the database name. If the database name begins with "DEV_", it adjusts the settings to development-specific values and enforces test mode, which restricts the export to a maximum of 10 customer records to avoid excessive processing during testing.

Next, customer accounts are grouped into batches using the `ROW_NUMBER` and `CEIL` functions, with each group containing up to `v_BatchSize` records (defaulted to 10,000). The grouped records are assigned `File_Group` values in the source table for systematic processing.

To export the data, the procedure dynamically constructs and runs queries to identify distinct `File_Group` values for records that haven’t been exported. Using a `FOR` loop, it iterates through each group, generating a JSON file for each batch and uploading it to Azure Blob Storage. The `COPY INTO` command facilitates this step, aggregating customer data with the `ARRAY_AGG` and `OBJECT_CONSTRUCT` functions. File sizes are capped at 50 MB for easy handling.

After successful export, the procedure updates the master customer table by increasing the export count for each account. It also marks exported records in the source table as sent and updates their modification timestamps.

Upon completion, the procedure returns a message summarizing the number of customer accounts exported. If no data is exported, it indicates this outcome. Errors are logged through an auditing mechanism, ensuring robust error handling by re-raising exceptions.

This well-designed procedure ensures seamless functionality in both development and production environments, effectively handles data batching, and includes comprehensive error management. A notable improvement would be to address the `TODO` comment associated with disabling the `v_TestRun` flag in production.
