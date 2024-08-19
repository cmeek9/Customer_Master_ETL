SELECT DISTINCT
	RTRIM(LTRIM(Email)) AS Customer_Email,
	CASE WHEN LEFT(CustomerNo,1) NOT LIKE '$' THEN CustomerNo END AS Customer_Number,
	CASE WHEN LEFT(CustomerNo,1) NOT LIKE '$' THEN CustomerName END AS Customer_Name,
	NULL AS Customer_Type,
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
    NULL AS Customer_Street_Address,
    NULL AS Customer_ZipCode,
    NULL AS Customer_Class,
    NULL AS Customer_Division,
	NULL AS [Parent_Customer_Name],
    NULL AS [is_deleted_ind],
	NULL AS [Prospect_Customer_ID],
	NULL AS [Prospect_Customer_Name],
    NULL AS Customer_Acquired_date,
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
	Phone AS [Customer_Phone_Number], 
	'SalesLink' AS [Source_DB],
    'OpportunityHeaderDelta' AS [Source_Table],
	GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM
	Opportunity.Opportunity_Header_Delta
WHERE
	LEFT(CustomerNo,1) NOT LIKE '$' AND CustomerNo <> 'NON-DBS'