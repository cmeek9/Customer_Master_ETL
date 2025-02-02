SELECT DISTINCT
        NULL AS Customer_Name
      ,[CustomerNo] AS Customer_Number
      ,[Division] AS Customer_Division
      ,[CustomerSize] AS Customer_Size
      ,[IndustryCode01] AS Customer_Industry_Code
      ,[PAR01] AS Plan_Annual_Rate_1
      ,[SalesRepNo01] AS Sales_Person_Number_1
      ,[SalesRepType01] AS Sales_Person_Type_1
      ,[PAR02] AS Plan_Annual_Rate_2
      ,[SalesRepNo02] AS Sales_Person_Number_2
      ,[SalesRepType02] AS Sales_Person_Type_2
      ,[PAR03] AS Plan_Annual_Rate_3
      ,[SalesRepNo03] AS Sales_Person_Number_3
      ,[SalesRepType03] AS Sales_Person_Type_3
      ,[PAR04] AS Plan_Annual_Rate_4
      ,[SalesRepNo04] AS Sales_Person_Number_4
      ,[SalesRepType04] AS Sales_Person_Type_4
      ,'SalesLink' AS [Source_DB]
      ,'avDL_DW_CustomerDivision' AS [Source_Table]
	  ,GETDATE() AS Source_Timestamp
      ,NULL AS Validated
  FROM [Cloudlink].[Customer].[avDL_DW_CustomerDivision]