SELECT DISTINCT
	RTRIM(LTRIM(Email)) AS Customer_Email,
	CustomerNo AS Customer_Number,
	CustomerName AS Customer_Name,
	NULL AS Customer_Type,
    Address1 AS Customer_Street_Address,
    ZipCode AS Customer_ZipCode,
    City AS [Customer_City],
    [State] AS [Customer_State],
    NULL AS [Customer_Country],
    NULL AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    NULL AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    SalesRepUserId AS [Customer_Sales_Rep],
    TermsCode AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	NULL AS [Prospect_Customer_ID],
	NULL AS [Prospect_Customer_Name],
    NULL AS Customer_Acquired_date,
    NULL AS Customer_Class,
    NULL AS Customer_Division,
    ContactName AS Full_Name,
    CASE 
        WHEN ContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', ContactName) > 0 THEN
            LEFT(ContactName, CHARINDEX(' ', ContactName) - 1)
        ELSE ContactName
    END AS First_Name,
    CASE 
        WHEN ContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', ContactName) > 0 THEN
            SUBSTRING(ContactName, 
                      CHARINDEX(' ', ContactName) + 1, 
                      LEN(ContactName))
        ELSE NULL
    END AS Last_Name,
	[CustomerPhone] AS [Customer_Phone_Number], 
	'SalesLink' AS [Source_DB],
    'PartsServiceQuoteHeader' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[PartsServiceQuote].[Quote_Header]
WHERE
	LEFT(CustomerNo,1) NOT LIKE '$' AND CustomerNo <> 'NON-DBS'