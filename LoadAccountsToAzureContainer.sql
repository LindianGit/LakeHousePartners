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
    -- This is the initial 'throttle' setting, limiting the number of customer sent for extraction.
    -- turn the tes mode off after we are confident customer account extraction is working as expected.
    v_TestRun               BOOLEAN         DEFAULT TRUE;   -- TODO: change to FALSE in DSSDRP-525
 
BEGIN
 
    IF (LEFT(CURRENT_DATABASE(),4) = 'DEV_') THEN
        v_Process           := 'DEV_' || v_Process;
        v_MasterCustomer    := 'DEV_' || v_MasterCustomer;
        v_Source            := 'DEV_' || v_Source;
        v_BIBackOfficeDB    := 'DEV_' || v_BIBackOfficeDB;
        v_TestRun           := TRUE;                        -- forcing TestRun in development databases
    END IF;
 
    -- When running in DEV or in test mode
    -- limit excessive exports by retaining only 10 customers to export
    IF (v_TestRun = TRUE) THEN
        DELETE FROM IDENTIFIER(:v_Source)
        WHERE Registered_Customer_Id_Upper NOT IN (
            SELECT TOP 10 Registered_Customer_Id_Upper FROM IDENTIFIER(:v_Source) WHERE Is_Sent = FALSE
            )
          AND Is_Sent = FALSE        -- do not delete customers already exported
          ;
    END IF;
 
    -- Calculate File_Group for customer accounts to be exported, batching by v_BatchSize records (currently 10k)
    -- we're only looking for "unsent" records here.
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
 
    -- we should only have one group here in TestRun
    LET v_Script := 'SELECT DISTINCT File_Group FROM ' || v_Source || ' WHERE Is_Sent = FALSE ORDER BY File_Group;';
 
    v_ResultSet := (EXECUTE IMMEDIATE :v_Script);
 
    -- Looping through FILE GROUP combination to produce the files.
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
            MAX_FILE_SIZE = 50000000;';     -- limiting file size to ~50MB (per batch, 10,000 customers max)
 
        EXECUTE IMMEDIATE v_SQLstmnt;
 
    END FOR;
 
    --update the number of times the customer account has been sent to azure for sanitisation.
    UPDATE IDENTIFIER(:v_MasterCustomer) mc
        SET mc.Sent_To_Azure_Count = mc.Sent_To_Azure_Count + 1
    FROM IDENTIFIER(:v_Source) ex
    WHERE   ex.Registered_Customer_Id_Upper = mc.Registered_Customer_Id_Upper
        AND ex.Country_Code = mc.Country_Code_Upper
        AND ex.Is_Sent = FALSE;
 
    --- Update Export/Stage records as "exported to Blob".
    UPDATE IDENTIFIER(:v_Source) ex
        SET ex.Status = TRUE
          , ex.DateModified_UTC = :v_ExecutionTimestamp
    WHERE ex.Is_Sent = FALSE;
 
    ------- Output the result -------
    IF (SQLFOUND = TRUE) THEN
        RETURN 'Success : Number of Customer accounts sent to Blob: ' || SQLROWCOUNT;
    ELSEIF (SQLNOTFOUND = TRUE) THEN
        RETURN 'Success : No rows sent to Blob.';
    ELSE
        RETURN 'Success : No DML statement triggered.'; -- DML : INSERT, UPDATE, DELETE, MERGE, COPY INTO etc...
    END IF;
 
EXCEPTION
    WHEN OTHER THEN
  -- Construct error details message for logging
    LET v_ErrorMsg := 'SQLCODE:' || SQLCODE || '== SQLSTATE:' || SQLSTATE || ' == SQLERRM: ' || SQLERRM;
    LET v_LoggingStatement := 'CALL ' || v_BIBackOfficeDB || '.audit.sp_ExecutionLog(?, ''ERROR'', 0, ''Error occurred'', ?);';
    EXECUTE IMMEDIATE :v_LoggingStatement USING (v_Process, v_ErrorMsg);
 
    RAISE; -- re-raise same exception
 
END;
$$
 
 
