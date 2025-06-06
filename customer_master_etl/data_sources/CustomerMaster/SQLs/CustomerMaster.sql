-- SELECT DISTINCT 
--     t1.[CUNO] AS Customer_Number,
--     t1.[CUNM] AS Customer_Name,
--     t1.[DIVI] AS Customer_Division,
--     t1.CUTYP AS Customer_Type,
--     t1.CUADD2 AS Customer_Street_Address,
--     t1.ZIPCD9 AS Customer_ZipCode,
--     t1.CUCYST AS [Customer_City],
--     t1.CUSTE AS [Customer_State],
--     t1.CCNTRY AS [Customer_Country],
--     t2.IDCD01 AS [Customer_Industry_Code],
--     NULL AS [Customer_Industry_Desc],
--     t1.CUSTST AS [Customer_Store_Number],
--     t1.CRMGR AS [Customer_Credit_Manager],
--     NULL AS [Customer_Credit_Rep],
--     NULL AS [Customer_Sales_Rep],
--     t1.TERMCD AS [Customer_terms_code],
--     t1.PRCUNO AS [Parent_Customer_Number],
-- 	NULL AS [Parent_Customer_Name],
--     t1.FLGDLI AS [is_deleted_ind],
--     t1.DTSYS8 AS Customer_Acquired_date,
--     t1.LOC AS Customer_Location,
--     t2.[CUSIZ2] AS Customer_Size,
--     t2.[PAR01] AS Plan_Annual_Rate_1,
--     t2.[SLMN01] AS Sales_Person_Number_1,
--     t2.[SLMT01] AS Sales_Person_Type_1,
--     t2.[PAR02] AS Plan_Annual_Rate_2,
--     t2.[SLMN02] AS Sales_Person_Number_2,
--     t2.[SLMT02] AS Sales_Person_Type_2,
--     t2.[PAR03] AS Plan_Annual_Rate_3,
--     t2.[SLMN03] AS Sales_Person_Number_3,
--     t2.[SLMT03] AS Sales_Person_Type_3,
--     t2.[PAR04] AS Plan_Annual_Rate_4,
--     t2.[SLMN04] AS Sales_Person_Number_4,
--     t2.[SLMT04] AS Sales_Person_Type_4,
--     CAST('CustomerMaster' AS VARCHAR(255)) AS [Source_DB],
--     CAST('CustomerMaster' AS VARCHAR(255)) AS [Source_Table],
--     GETDATE() AS Source_Timestamp,
--     NULL AS Validated
-- FROM
--     CIPNAME0 t1
-- LEFT JOIN 
-- 	SCPDIVF0 t2 
-- ON
-- 	t1.CUNO = t2.CUNO AND t1.DIVI = t2.DIVI


-- SELECT DISTINCT
--     t2.[CUNO] AS Customer_Number,
--     t2.[DIVI] AS Customer_Division,
--     t1.[CUNM] AS Customer_Name,
--     t1.CUTYP AS Customer_Type,
--     t1.CUADD2 AS Customer_Street_Address,
--     t1.ZIPCD9 AS Customer_ZipCode,
--     t1.CUCYST AS [Customer_City],
--     t1.CUSTE AS [Customer_State],
--     t1.CCNTRY AS [Customer_Country],
--     t2.IDCD01 AS [Customer_Industry_Code],
--     NULL AS [Customer_Industry_Desc],
--     t1.CUSTST AS [Customer_Store_Number],
--     t1.CRMGR AS [Customer_Credit_Manager],
--     NULL AS [Customer_Credit_Rep],
--     NULL AS [Customer_Sales_Rep],
--     t1.TERMCD AS [Customer_terms_code],
--     t1.PRCUNO AS [Parent_Customer_Number],
--     NULL AS [Parent_Customer_Name],
--     t1.FLGDLI AS [is_deleted_ind],
--     t1.DTSYS8 AS Customer_Acquired_date,
--     t1.LOC AS Customer_Location,
--     t2.[CUSIZ2] AS Customer_Size,
--     t2.[PAR01] AS Plan_Annual_Rate_1,
--     t2.[SLMN01] AS Sales_Person_Number_1,
--     t2.[SLMT01] AS Sales_Person_Type_1,
--     t2.[PAR02] AS Plan_Annual_Rate_2,
--     t2.[SLMN02] AS Sales_Person_Number_2,
--     t2.[SLMT02] AS Sales_Person_Type_2,
--     t2.[PAR03] AS Plan_Annual_Rate_3,
--     t2.[SLMN03] AS Sales_Person_Number_3,
--     t2.[SLMT03] AS Sales_Person_Type_3,
--     t2.[PAR04] AS Plan_Annual_Rate_4,
--     t2.[SLMN04] AS Sales_Person_Number_4,
--     t2.[SLMT04] AS Sales_Person_Type_4,
--     CAST('CustomerMaster' AS VARCHAR(255)) AS [Source_DB],
--     CAST('CustomerMaster' AS VARCHAR(255)) AS [Source_Table],
--     GETDATE() AS Source_Timestamp,
--     NULL AS Validated
-- FROM
--     SCPDIVF0 t2
-- LEFT JOIN 
--     CIPNAME0 t1  
-- ON
--     t1.CUNO = t2.CUNO



-- SELECT DISTINCT
--     cm.Customer_Number,
--     cm.Customer_Name,
--     div.Customer_Division,
--     cm.Customer_Type,
--     cm.Customer_Street_Address,
--     cm.Customer_ZipCode,
--     cm.Customer_City,
--     cm.Customer_State,
--     cm.Customer_Country,
--     cm.Customer_Store_Number,
--     cm.Customer_Credit_Manager,
--     cm.Customer_Terms_Code,
--     cm.Parent_Customer_Number,
--     cm.is_deleted_ind,
--     cm.Customer_Location,
--     div.Customer_Size,
--     div.Customer_Industry_Code,
--     div.Plan_Annual_Rate_1,
--     div.Sales_Person_Number_1,
--     div.Sales_Person_Type_1,
--     div.Plan_Annual_Rate_2,
--     div.Sales_Person_Number_2,
--     div.Sales_Person_Type_2,
--     div.Plan_Annual_Rate_3,
--     div.Sales_Person_Number_3,
--     div.Sales_Person_Type_3,
--     div.Plan_Annual_Rate_4,
--     div.Sales_Person_Number_4,
--     div.Sales_Person_Type_4,
--     COALESCE(cm.Source_DB, div.Source_DB) AS Source_DB,
--     COALESCE(cm.Source_Table, div.Source_Table) AS Source_Table,
--     GETDATE() AS Source_Timestamp
-- FROM
--     (
--         -- Customer Master Query
--         SELECT DISTINCT
--             t1.CUNO AS Customer_Number,
--             t1.CUNM AS Customer_Name,
--             t1.CUTYP AS Customer_Type,
--             t1.CUADD2 AS Customer_Street_Address,
--             t1.ZIPCD9 AS Customer_ZipCode,
--             t1.CUCYST AS Customer_City,
--             t1.CUSTE AS Customer_State,
--             t1.CCNTRY AS Customer_Country,
--             t1.CUSTST AS Customer_Store_Number,
--             t1.CRMGR AS Customer_Credit_Manager,
--             t1.TERMCD AS Customer_Terms_Code,
--             t1.PRCUNO AS Parent_Customer_Number,
--             t1.FLGDLI AS is_deleted_ind,
--             t1.DTSYS8 AS Customer_Acquired_date,
--             t1.LOC AS Customer_Location,
--             CAST('LIBE25' AS VARCHAR(255)) AS Source_DB,
--             CAST('CIPNAME0' AS VARCHAR(255)) AS Source_Table,
--             GETDATE() AS Source_Timestamp
--         FROM
--             CIPNAME0 t1
--     ) cm
-- LEFT JOIN
--     (
--         -- Division Data Query
--         SELECT DISTINCT
--             [CUNO] AS Customer_Number,
-- 			[DIVI] AS Customer_Division,
-- 			IDCD01 AS [Customer_Industry_Code],
-- 			[CUSIZ2] AS Customer_Size,
-- 			[PAR01] AS Plan_Annual_Rate_1,
-- 			[SLMN01] AS Sales_Person_Number_1,
-- 			[SLMT01] AS Sales_Person_Type_1,
-- 			[PAR02] AS Plan_Annual_Rate_2,
-- 			[SLMN02] AS Sales_Person_Number_2,
-- 			[SLMT02] AS Sales_Person_Type_2,
-- 			[PAR03] AS Plan_Annual_Rate_3,
-- 			[SLMN03] AS Sales_Person_Number_3,
-- 			[SLMT03] AS Sales_Person_Type_3,
-- 			[PAR04] AS Plan_Annual_Rate_4,
-- 			[SLMN04] AS Sales_Person_Number_4,
-- 			[SLMT04] AS Sales_Person_Type_4,
--             CAST('LIBE25' AS VARCHAR(255)) AS Source_DB,
--             CAST('SCPDIVF0' AS VARCHAR(255)) AS Source_Table,
--             GETDATE() AS Source_Timestamp
--         FROM
--             SCPDIVF0
--     ) div
-- ON cm.Customer_Number = div.Customer_Number




SELECT DISTINCT
    cm.Customer_Number,
    cm.Customer_Name,
    div.Customer_Division,
    cm.Customer_Type,
    cm.Customer_Street_Address,
    cm.Customer_ZipCode,
    cm.Customer_City,
    cm.Customer_State,
    cm.Customer_Country,
    cm.Customer_Store_Number,
    cm.Customer_Credit_Manager,
    cm.Customer_Terms_Code,
    cm.Parent_Customer_Number,
    cm.is_deleted_ind,
	cm.Customer_Acquired_date,
    cm.Customer_Location,
	cm.Parts_Customer_Class,
	cm.Labor_Customer_Class,
	cm.Misc_Customer_Class,
    div.Customer_Size,
    div.Customer_Industry_Code,
    div.Plan_Annual_Rate_1,
    div.Sales_Person_Number_1,
    div.Sales_Person_Type_1,
    div.Plan_Annual_Rate_2,
    div.Sales_Person_Number_2,
    div.Sales_Person_Type_2,
    div.Plan_Annual_Rate_3,
    div.Sales_Person_Number_3,
    div.Sales_Person_Type_3,
    div.Plan_Annual_Rate_4,
    div.Sales_Person_Number_4,
    div.Sales_Person_Type_4,
    COALESCE(cm.Source_DB, div.Source_DB) AS Source_DB,
    COALESCE(cm.Source_Table, div.Source_Table) AS Source_Table,
    GETDATE() AS Source_Timestamp
FROM
    (
        -- Customer Master Query
        SELECT DISTINCT
            t1.CUNO AS Customer_Number,
            t1.CUNM AS Customer_Name,
            t1.CUTYP AS Customer_Type,
            t1.CUADD2 AS Customer_Street_Address,
            t1.ZIPCD9 AS Customer_ZipCode,
            t1.CUCYST AS Customer_City,
            t1.CUSTE AS Customer_State,
            t1.CCNTRY AS Customer_Country,
            t1.CUSTST AS Customer_Store_Number,
            t1.CRMGR AS Customer_Credit_Manager,
            t1.TERMCD AS Customer_Terms_Code,
            t1.PRCUNO AS Parent_Customer_Number,
            t1.FLGDLI AS is_deleted_ind,
            t1.DTSYS8 AS Customer_Acquired_date,
            t1.LOC AS Customer_Location,
			t1.PACC AS Parts_Customer_Class,
			t1.LBCC AS Labor_Customer_Class,
			t1.MCCC AS Misc_Customer_Class,
            CAST('LIBE25' AS VARCHAR(255)) AS Source_DB,
            CAST('CIPNAME0' AS VARCHAR(255)) AS Source_Table,
            GETDATE() AS Source_Timestamp
        FROM
            CIPNAME0 t1
    ) cm
LEFT JOIN
    (
        -- Division Data Query
        SELECT DISTINCT
            [CUNO] AS Customer_Number,
			[DIVI] AS Customer_Division,
			IDCD01 AS [Customer_Industry_Code],
			[CUSIZ2] AS Customer_Size,
			[PAR01] AS Plan_Annual_Rate_1,
			[SLMN01] AS Sales_Person_Number_1,
			[SLMT01] AS Sales_Person_Type_1,
			[PAR02] AS Plan_Annual_Rate_2,
			[SLMN02] AS Sales_Person_Number_2,
			[SLMT02] AS Sales_Person_Type_2,
			[PAR03] AS Plan_Annual_Rate_3,
			[SLMN03] AS Sales_Person_Number_3,
			[SLMT03] AS Sales_Person_Type_3,
			[PAR04] AS Plan_Annual_Rate_4,
			[SLMN04] AS Sales_Person_Number_4,
			[SLMT04] AS Sales_Person_Type_4,
            CAST('LIBE25' AS VARCHAR(255)) AS Source_DB,
            CAST('SCPDIVF0' AS VARCHAR(255)) AS Source_Table,
            GETDATE() AS Source_Timestamp
        FROM
            SCPDIVF0
    ) div
ON cm.Customer_Number = div.Customer_Number