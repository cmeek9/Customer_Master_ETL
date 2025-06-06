-- SELECT [customerId] AS Customer_Number
--       ,[firstName] AS First_Name
--       ,[lastName] AS Last_Name
--       ,[name] AS Full_Name
--       ,[phone] AS Customer_Phone_Number
--       ,[mainDivision] AS Customer_Division
--       ,[divisions] AS Division_List
--       ,[typeCode] AS [Type_Code]
--       ,[typeDescription] AS [Type_Description]
--       ,[email] AS Customer_Email
--       ,ISNULL([address1], '') + ISNULL([address2], '') + ISNULL([address3], '') AS [Customer_Street_Address]
--       ,[city] AS [Customer_City]
--       ,[state] AS [Customer_State]
--       ,[zipCode] AS [Customer_ZipCode]
--       ,'Cloudlink' AS Source_DB
-- 	  ,'Contacts' AS Source_Table
-- 	  ,GETDATE() AS Source_Timestamp
--   FROM [Cloudlink].[Contact].[Contacts]
--   WHERE [customerId] NOT LIKE '$%'


SELECT [customerId] AS Customer_Number
      ,[firstName] AS First_Name
      ,[lastName] AS Last_Name
      ,[name] AS Full_Name
      ,[phone] AS Customer_Phone_Number
	,[mobilePhone] AS [Customer_Mobile_Phone]
      ,[mainDivision] AS Customer_Division
      ,[divisions] AS Division_List
      ,[typeCode] AS [Type_Code]
      ,[typeDescription] AS [Type_Description]
	,[title] AS [Job_Title]
      ,[email] AS Customer_Email
      ,ISNULL([address1], '') + ISNULL([address2], '') + ISNULL([address3], '') AS [Customer_Street_Address]
      ,[city] AS [Customer_City]
      ,[state] AS [Customer_State]
      ,[zipCode] AS [Customer_ZipCode]
      ,'Cloudlink' AS Source_DB
	,'Contacts' AS Source_Table
	,GETDATE() AS Source_Timestamp
  FROM [Cloudlink].[Contact].[Contacts]
  WHERE [customerId] NOT LIKE '$%'