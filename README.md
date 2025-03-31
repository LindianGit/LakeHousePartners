 
LoadAccountsToAzureContainer Stored Procedure

 Overview
 ==========

The `LoadAccountsToAzureContainer` stored procedure is designed to upload inactive customer accounts to Azure Blob Storage. 
This procedure processes customer data in batches, converts it to JSON format, and uploads the data to a specified Blob Storage location. 
It also updates the status of the records in the source tables accordingly.


Procedure Definition

 Features
 ==========

The Snowflake SQL procedure `LoadAccountsToAzureContainer` automates the export of inactive customer accounts to Azure Blob Storage in JSON format. 
It ensures efficient data transfer with structured processes for preparation, batching, export, and logging.

Key features include:
- **Development/Test Mode:** The `v_TestRun` flag limits record processing for safe testing in non-production environments, retaining only 10 records during testing.
- **Batching:** Accounts are grouped into batches of up to 10,000 (`v_BatchSize`) using `ROW_NUMBER` and `CEIL`, and the source table is updated with `File_Group` values.
- **Data Export:** A `FOR` loop iterates through each batch, exporting data to JSON files in Azure via the `COPY INTO` command, with each file capped at 50 MB for manageability.
- **Post-Export Updates:** The master customer table is updated to reflect export status, including incremented send counts and modification timestamps.
- **Error Handling:** Errors are logged using an audit mechanism, and exceptions are re-raised for proper handling.
- Processes and exports inactive customer accounts in batches.
- Converts customer data to JSON format for Blob Storage.
- Supports test mode to limit data processing during development.
- Updates the status of processed records in the source tables.
- Logs errors and execution details for auditing purposes.


--===============================================================================================================================================================================================

 
CREATE PROCEDURE LoadAccountsToAzureContainer
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'This procedure is used to upload inactive customer accounts to Blob Storage.'
AS
$$
/***************************************************************************************************************************
Copy JSON to Blob example
****************************************************************************************************/

DECLARE

    v_BIBackOfficeDB        VARCHAR         DEFAULT 'BI_BACKOFFICE';
    v_MasterCustomer        VARCHAR         DEFAULT 'COMPLIANCE.Sanitisation.Master_Customer';
    v_Source                VARCHAR         DEFAULT 'COMPLIANCE.Sanitisation.Azure_Export';
    v_Process               VARCHAR         DEFAULT 'COMPLIANCE.Sanitisation.SP_Load_Inactive_Customer_Accounts_To_Blob';
    v_ExtStage              VARCHAR         DEFAULT '@Config.LandingZone_Sanitisation_Customer_Accounts/Inactive_Customer_Account';
    v_BatchSize             INT             DEFAULT 10000;

    v_ExecutionTimestamp    TIMESTAMP_NTZ   DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP());
    v_TargetFileName        VARCHAR;
    v_ResultSet             RESULTSET;
    v_File_Group            INTEGER;
    v_SQLstmnt              VARCHAR;
    v_TestRun               BOOLEAN         DEFAULT TRUE;

BEGIN

    IF (LEFT(CURRENT_DATABASE(),4) = 'DEV_') THEN
        v_Process           := 'DEV_' || v_Process;
        v_MasterCustomer    := 'DEV_' || v_MasterCustomer;
        v_Source            := 'DEV_' || v_Source;
        v_BIBackOfficeDB    := 'DEV_' || v_BIBackOfficeDB;
        v_TestRun           := TRUE;
    END IF;

    IF (v_TestRun = TRUE) THEN
        DELETE FROM IDENTIFIER(:v_Source)
        WHERE Registered_Customer_Id_Upper NOT IN (
            SELECT TOP 10 Registered_Customer_Id_Upper FROM IDENTIFIER(:v_Source) WHERE Is_Sent = FALSE
            )
          AND Is_Sent = FALSE;
    END IF;

    UPDATE IDENTIFIER(:v_Source) target
        SET target.File_Group = src.File_Group
    FROM (
        SELECT
            Registered_Customer_Id_Upper,
            CEIL(ROW_NUMBER() OVER (ORDER BY Registered_Customer_Id_Upper) / v_BatchSize) AS File_Group
        FROM IDENTIFIER(:v_Source)
        WHERE Is_Sent = FALSE
        ) src
    WHERE target.Registered_Customer_Id_Upper = src.Registered_Customer_Id_Upper;

    LET v_Script := 'SELECT DISTINCT File_Group FROM ' || v_Source || ' WHERE Is_Sent = FALSE ORDER BY File_Group;';

    v_ResultSet := (EXECUTE IMMEDIATE :v_Script);

    FOR row_variable IN v_ResultSet DO

        v_File_Group := row_variable.File_Group;
        v_TargetFileName := v_ExtStage || '_' || v_File_Group || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'yyyy-MM-dd_HHMISS') || '.json';

        v_SQLstmnt := '
            COPY INTO ' || v_TargetFileName || '
            FROM (
                SELECT
                    ARRAY_AGG(
                        OBJECT_CONSTRUCT(
                            ''customerId'', Registered_Customer_Id_Upper,
                            ''lastOrderDate'', Last_Order_Date,
                            ''countryCode'', Country_Code_Upper,
                            ''retentionPeriodDays'', Retention_Period_Days
                        )
                    ) AS json_value
                FROM ' || v_Source || ' AS al
                WHERE al.File_Group =''' || v_File_Group || '''
                  AND Is_Sent = FALSE
            )
            FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
            OVERWRITE = TRUE
            SINGLE = TRUE
            MAX_FILE_SIZE = 50000000;';

        EXECUTE IMMEDIATE v_SQLstmnt;

    END FOR;

    UPDATE IDENTIFIER(:v_MasterCustomer) mc
        SET mc.Sent_To_Azure_Count = mc.Sent_To_Azure_Count + 1
    FROM IDENTIFIER(:v_Source) ex
    WHERE   ex.Registered_Customer_Id_Upper = mc.Registered_Customer_Id_Upper
        AND ex.Country_Code = mc.Country_Code_Upper
        AND ex.Is_Sent = FALSE;

    UPDATE IDENTIFIER(:v_Source) ex
        SET ex.Status = TRUE
          , ex.DateModified_UTC = :v_ExecutionTimestamp
    WHERE ex.Is_Sent = FALSE;

    IF (SQLFOUND = TRUE) THEN
        RETURN 'Success: Number of Customer accounts sent to Blob: ' || SQLROWCOUNT;
    ELSEIF (SQLNOTFOUND = TRUE) THEN
        RETURN 'Success: No rows sent to Blob.';
    ELSE
        RETURN 'Success: No DML statement triggered.';
    END IF;

EXCEPTION
    WHEN OTHER THEN
        LET v_ErrorMsg := 'SQLCODE:' || SQLCODE || '== SQLSTATE:' || SQLSTATE || ' == SQLERRM: ' || SQLERRM;
        LET v_LoggingStatement := 'CALL ' || v_BIBackOfficeDB || '.audit.sp_ExecutionLog(?, ''ERROR'', 0, ''Error occurred'', ?);';
        EXECUTE IMMEDIATE :v_LoggingStatement USING (v_Process, v_ErrorMsg);

        RAISE;
END;
$$


Usage

Parameters
==========

- `v_BIBackOfficeDB`: Name of the back-office database (default: 'BI_BACKOFFICE').
- `v_MasterCustomer`: Name of the master customer table (default: 'COMPLIANCE.Sanitisation.Master_Customer').
- `v_Source`: Name of the source table containing customer data (default: 'COMPLIANCE.Sanitisation.Azure_Export').
- `v_Process`: Name of the stored procedure (default: 'COMPLIANCE.Sanitisation.SP_Load_Inactive_Customer_Accounts_To_Blob').
- `v_ExtStage`: External stage for storing the JSON files (default: `@Config.LandingZone_Sanitisation_Customer_Accounts/Inactive_Customer_Account`).
- `v_BatchSize`: Batch size for processing customer accounts (default: 10000).
- `v_TestRun`: Boolean flag for test mode (default: TRUE).

Execution
==========

To execute the stored procedure, use the following command:

 
CALL LoadAccountsToAzureContainer();
 

Error Handling
===============
In case of an error, the procedure logs the error details to an audit table and re-raises the exception.

 
