--contains all prospects from Cloudlink into a table with a union for everything

SELECT DISTINCT --good
	RTRIM(LTRIM([Email])) AS Customer_Email,
	NULL AS Customer_Number,
    NULL AS Customer_Name,
	NULL AS Customer_Type,
    [address2] AS Customer_Street_Address,
    [zipCode] AS Customer_ZipCode,
    [city] AS [Customer_City],
    [state] AS [Customer_State],
    [country] AS [Customer_Country],
    NULL AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    NULL AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    NULL AS [Customer_Sales_Rep],
    [termsCode] AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	CASE WHEN type = 'Prospect' THEN [id] END  AS [Prospect_Customer_Number],
	CASE WHEN type = 'Prospect' THEN [name] END AS [Prospect_Customer_Name],
    NULL AS Customer_Acquired_date,
    NULL AS Customer_Location,
    NULL AS Customer_Division,
    [Phone] AS [Customer_Phone_Number], 
    NULL AS Full_Name,
    NULL AS First_Name,
    NULL AS Last_Name,
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'Customers' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[Customer].[Customers]
WHERE 
	type = 'Prospect'


UNION


SELECT DISTINCT --good
	RTRIM(LTRIM(CustomerContacEmail)) AS Customer_Email,
	NULL AS Customer_Number,
	NULL AS Customer_Name,
	[CustomerTypeId] AS Customer_Type,
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
    SalesRepID AS [Customer_Sales_Rep],
    NULL AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
	NULL AS [is_deleted_ind],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerNo END AS [Prospect_Customer_Number],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerName END AS [Prospect_Customer_Name],
	[EnterDate] AS Customer_Acquired_date,
    NULL AS Customer_Location,
    [Division] AS Customer_Division,
    [CustomerContacPhone] AS [Customer_Phone_Number], 
    CustomerContactName AS Full_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            LEFT(CustomerContactName, CHARINDEX(' ', CustomerContactName) - 1)
        ELSE CustomerContactName
    END AS First_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            SUBSTRING(CustomerContactName, 
                      CHARINDEX(' ', CustomerContactName) + 1, 
                      LEN(CustomerContactName))
        ELSE NULL
    END AS Last_Name,
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'EquipmentQuoteHeader' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[EquipmentQuote].[Quote_Header]
WHERE
	LEFT(CustomerNo,1) LIKE '$'


UNION
	

SELECT DISTINCT -- good
	RTRIM(LTRIM(CustomerContacEmail)) AS Customer_Email,
	NULL AS Customer_Number,
	NULL AS Customer_Name,
	CustomerTypeId AS Customer_Type,
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
    SalesRepID AS [Customer_Sales_Rep],
    NULL AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
    NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
    CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerNo END AS [Prospect_Customer_Number],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerName END AS [Prospect_Customer_Name],
    [EnterDate] AS Customer_Acquired_date,
    NULL AS Customer_Location,
    [Division] AS Customer_Division,
    [CustomerContacPhone] AS [Customer_Phone_Number], 
    CustomerContactName AS Full_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            LEFT(CustomerContactName, CHARINDEX(' ', CustomerContactName) - 1)
        ELSE CustomerContactName
    END AS First_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            SUBSTRING(CustomerContactName, 
                      CHARINDEX(' ', CustomerContactName) + 1, 
                      LEN(CustomerContactName))
        ELSE NULL
    END AS Last_Name,
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'EquipmentQuoteHeaderDelta' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[EquipmentQuote].[Quote_Header_Delta]
WHERE
	LEFT(CustomerNo,1) LIKE '$'

UNION

SELECT DISTINCT --good
	RTRIM(LTRIM(Email)) AS Customer_Email,
	NULL AS Customer_Number,
	NULL AS Customer_Name,
	NULL AS Customer_Type,
    [Address] AS Customer_Street_Address,
    [PostalCode] AS Customer_ZipCode,
    [City] AS [Customer_City],
    [ProvinceState] AS [Customer_State],
    [Country] AS [Customer_Country],
    NULL AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    [BranchNo] AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    SalesRepID AS [Customer_Sales_Rep],
    NULL AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
    NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerNo END AS [Prospect_Customer_Number],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerName END AS [Prospect_Customer_Name],
    [EnterDate] AS Customer_Acquired_date,
    NULL AS Customer_Location,
    [Division] AS Customer_Division,
    Phone AS [Customer_Phone_Number], 
    CustomerContactName AS Full_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            LEFT(CustomerContactName, CHARINDEX(' ', CustomerContactName) - 1)
        ELSE CustomerContactName
    END AS First_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            SUBSTRING(CustomerContactName, 
                      CHARINDEX(' ', CustomerContactName) + 1, 
                      LEN(CustomerContactName))
        ELSE NULL
    END AS Last_Name,
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'Opportunity_Header' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	Opportunity.Opportunity_Header
WHERE
	LEFT(CustomerNo,1) LIKE '$'

UNION

SELECT DISTINCT -- good
	RTRIM(LTRIM(Email)) AS Customer_Email,
	NULL AS Customer_Number,
    NULL AS Customer_Name,
	NULL AS Customer_Type,
    [Address] AS Customer_Street_Address,
    [PostalCode] AS Customer_ZipCode,
    [City] AS [Customer_City],
    [ProvinceState] AS [Customer_State],
    [Country] AS [Customer_Country],
    NULL AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    NULL AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    SalesRepID AS [Customer_Sales_Rep],
    NULL AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerNo END AS [Prospect_Customer_Number],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerName END AS [Prospect_Customer_Name],
    [EnterDate] AS Customer_Acquired_date,
    NULL AS Customer_Location,
    [Division] AS Customer_Division,
    Phone AS [Customer_Phone_Number], 
    CustomerContactName AS Full_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            LEFT(CustomerContactName, CHARINDEX(' ', CustomerContactName) - 1)
        ELSE CustomerContactName
    END AS First_Name,
    CASE 
        WHEN CustomerContactName IS NULL THEN NULL
        WHEN CHARINDEX(' ', CustomerContactName) > 0 THEN
            SUBSTRING(CustomerContactName, 
                      CHARINDEX(' ', CustomerContactName) + 1, 
                      LEN(CustomerContactName))
        ELSE NULL
    END AS Last_Name,
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'OpportunityHeaderDelta' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM
	Opportunity.Opportunity_Header_Delta
WHERE
	LEFT(CustomerNo,1) LIKE '$'

UNION

SELECT DISTINCT -- good
	RTRIM(LTRIM(Email)) AS Customer_Email,
	NULL AS Customer_Number,
    NULL AS Customer_Name,
	NULL AS Customer_Type,
    COALESCE(Address1, Address2) AS Customer_Street_Address,
    ZipCode AS Customer_ZipCode,
    City AS [Customer_City],
    [State] AS [Customer_State],
    NULL AS [Customer_Country],
    [IndustryCode] AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    NULL AS [Customer_Store_Number],
    NULL AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    SalesRepUserId AS [Customer_Sales_Rep],
    TermsCode AS [Customer_terms_code],
    NULL AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerNo END AS [Prospect_Customer_Number],
	CASE WHEN LEFT(CustomerNo,1) LIKE '$' THEN CustomerName END AS [Prospect_Customer_Name],
    [EnterDate] AS Customer_Acquired_date,
    NULL AS Customer_Location,
    [Division] AS Customer_Division,
    [CustomerPhone] AS [Customer_Phone_Number], 
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
    NULL AS CAT_DCN, --copy from here
    NULL AS CAT_Customer_Name,
    NULL AS CAT_DCN_City,
    NULL AS DCN_CWS_ID_Created,
    NULL AS DCN_Created_Date,
    NULL AS DCN_Last_Modified_Date,
    NULL AS DCN_Last_Modified_CWS_ID,
    NULL AS UCID_Created_By_CWS,
    NULL AS CAT_UCID,
    NULL AS UCID_Customer_Name,
    NULL AS UNC_Marketing_Customer_Name,
    NULL AS [UCID_Created_Date],
    NULL AS [UCID_Customer_Address],
    NULL AS [UCID_Customer_City],
    NULL AS [UCID_Customer_State],
    NULL AS [UCID_Customer_ZipCode],
    NULL AS [UCID_Customer_Country],
    NULL AS [UCID_Customer_Email],
    NULL AS [CAT_CCID],
    NULL AS [CAT_CWS_ID],
	'SalesLink' AS [Source_DB],
    'PartsServiceQuoteHeader' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM 
	[PartsServiceQuote].[Quote_Header]
WHERE
	LEFT(CustomerNo,1) LIKE '$'