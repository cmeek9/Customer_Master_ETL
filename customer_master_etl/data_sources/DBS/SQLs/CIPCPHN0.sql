SELECT DISTINCT 
	a.cuno AS Customer_Number,
	b.cunm  AS Customer_Name,
    a.phneml AS Customer_Email,
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
    NULL AS Customer_Size,
    NULL AS Plan_Annual_Rate_1,
    NULL AS Sales_Person_Numbe_1,
    NULL AS Sales_Person_Type_1,
    NULL AS Plan_Annual_Rate_2,
    NULL AS Sales_Person_Number_2,
    NULL AS Sales_Person_Type_2,
    NULL AS Plan_Annual_Rate_3,
    NULL AS Sales_Person_Number_3,
    NULL AS Sales_Person_Type_3,
    NULL AS Plan_Annual_Rate_4,
    NULL AS Sales_Person_Number_4,
    NULL AS Sales_Person_Type_4,
    NULL AS Customer_Acquired_date,
    NULL AS Customer_Location,
    NULL AS Customer_Division,
    NULL AS Full_Name,
    CAST('DBS' AS VARCHAR(255)) AS [Source_DB],
    CAST('CIPCPHN0' AS VARCHAR(255)) AS [Source_Table],
    GETDATE() AS Source_Timestamp,
    NULL AS Validated
FROM
    cipcphn0 a
LEFT JOIN
    cipname0 b ON a.cuno = b.cuno