WITH SourceData AS (
    SELECT *
    FROM OPENQUERY(AS400, 
        'SELECT LHEMAIL, LHCUNO, LHAREA, LHPREFIX, LHLINE, LHFIRST, LHLAST 
         FROM CATs7T.DBSLIB500.LHLDBSDCNT'
    )
)

SELECT DISTINCT
	RTRIM(LTRIM(LHEMAIL)) AS Customer_Email,
	LHCUNO AS Customer_Number,
	NULL AS Customer_Name,
	NULL AS Customer_Type,
    NULL AS Customer_Street_Address,
    NULL AS Customer_ZipCode,
    NULL AS [Customer_City],
    NULL AS [Customer_State],
    NULL AS [Customer_Country],
    NULL AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    NULL AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    NULL AS [Customer_Sales_Rep],
    NULL AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	NULL AS [Prospect_Customer_Number],
	NULL AS [Prospect_Customer_Name],
    NULL AS Customer_Acquired_date,
    NULL AS Customer_Location,
    NULL AS Customer_Division,
    NULL AS Full_Name,
    CONCAT(LHAREA, ' ', LHPREFIX, ' ', LHLINE) AS [Customer_Phone_Number], 
    LHFIRST AS First_Name,
    LHLAST AS Last_Name,
	'DBSLIB500' AS [Source_DB],
    'LHLDBSDCNT' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	SourceData