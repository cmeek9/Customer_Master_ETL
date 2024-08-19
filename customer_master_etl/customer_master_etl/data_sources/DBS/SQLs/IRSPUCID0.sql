SELECT 
	NULL AS 'Customer_Number',
	UCINAME AS Customer_Name,
	UCICNTREML AS Customer_Email,
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
	NULL AS [Prospect_Customer_ID],
	NULL AS [Prospect_Customer_Name],
    NULL AS Customer_Acquired_date,
    NULL AS Customer_Class,
    NULL AS Customer_Division,
    NULL AS Full_Name,
	CAST('DBS' AS VARCHAR(255)) AS [Source_DB],
    CAST('IRSPUCID0' AS VARCHAR(255)) AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM OPENQUERY(as400, 'SELECT DISTINCT UCINAME, UCICNTREML FROM CATS7T.LIBE25.IRSPUCID0 ')
