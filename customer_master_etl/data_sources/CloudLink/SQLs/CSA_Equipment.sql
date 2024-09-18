SELECT DISTINCT
	RTRIM(LTRIM(SMUContactEmail)) AS Customer_Email,
	CustomerNumber AS Customer_Number,
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
    SMUContact AS Full_Name,
    CASE 
        WHEN SMUContact IS NULL THEN NULL
        WHEN CHARINDEX(' ', SMUContact) > 0 THEN
            LEFT(SMUContact, CHARINDEX(' ', SMUContact) - 1)
        ELSE SMUContact
    END AS First_Name,
    CASE 
        WHEN SMUContact IS NULL THEN NULL
        WHEN CHARINDEX(' ', SMUContact) > 0 THEN
            SUBSTRING(SMUContact, 
                      CHARINDEX(' ', SMUContact) + 1, 
                      LEN(SMUContact))
        ELSE NULL
    END AS Last_Name,
	[SMUContactPhone] AS [Customer_Phone_Number], 
	'SalesLink' AS [Source_DB],
    'CSA_Equipment' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[CSA].[CSA_Equipment]
