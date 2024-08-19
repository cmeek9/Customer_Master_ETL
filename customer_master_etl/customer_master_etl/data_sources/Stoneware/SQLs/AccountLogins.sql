SELECT DISTINCT
    t1.[EmailAddress] AS Customer_Email,
    CASE
      WHEN LEFT([DefaultCustomerAccountKey], 1) LIKE '[A-Za-z]' 
        THEN SUBSTRING([DefaultCustomerAccountKey], 2, LEN([DefaultCustomerAccountKey]) - 1)
        ELSE [DefaultCustomerAccountKey]
    END AS Customer_Number,
    CompanyName AS Customer_Name,
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
    t1.[Name] AS Full_Name,
    t1.[FirstName] AS First_Name,
    t1.[LastName] AS Last_Name,
    t1.[MobilePhone] AS Customer_Phone_Number,
    -- ,t1.[WorkPhone]
    -- ,t1.[Department]
    -- ,t1.[Title]
    'Stoneware' AS [Source_DB],
    'AccountCompany' AS [Source_Table],
    -- t1.[Inactive],
    GETDATE() AS Source_Timestamp
  FROM [Stoneware].[dbo].[AccountLogins] t1
  LEFT JOIN [Stoneware].[dbo].[AccountCompany] t2
  ON t1.CompanyID = t2.CompanyID
  where [DefaultCustomerAccountKey] IS NOT NULL