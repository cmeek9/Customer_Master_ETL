SELECT DISTINCT 
    t1.[CUNO] AS Customer_Number,
    t1.[CUNM] AS Customer_Name,
    t1.[DIVI] AS Customer_Division,
    t1.CUTYP AS Customer_Type,
    t1.CUADD2 AS Customer_Street_Address,
    t1.ZIPCD9 AS Customer_ZipCode,
    t1.CUCYST AS [Customer_City],
    t1.CUSTE AS [Customer_State],
    t1.CCNTRY AS [Customer_Country],
    t2.IDCD01 AS [Customer_Industry_Code],
    NULL AS [Customer_Industry_Desc],
    t1.CUSTST AS [Customer_Store_Number],
    t1.CRMGR AS [Customer_Credit_Manager],
    NULL AS [Customer_Credit_Rep],
    NULL AS [Customer_Sales_Rep],
    t1.TERMCD AS [Customer_terms_code],
    t1.PRCUNO AS [Parent_Customer_Number],
	NULL AS [Parent_Customer_Name],
    t1.FLGDLI AS [is_deleted_ind],
    t1.DTSYS8 AS Customer_Acquired_date,
    t1.LOC AS Customer_Class,
    t2.[CUSIZ2] AS Customer_Size,
    t2.[PAR01] AS Plan_Annual_Rate_1,
    t2.[SLMN01] AS Sales_Person_Numbe_1,
    t2.[SLMT01] AS Sales_Person_Type_1,
    t2.[PAR02] AS Plan_Annual_Rate_2,
    t2.[SLMN02] AS Sales_Person_Number_2,
    t2.[SLMT02] AS Sales_Person_Type_2,
    t2.[PAR03] AS Plan_Annual_Rate_3,
    t2.[SLMN03] AS Sales_Person_Number_3,
    t2.[SLMT03] AS Sales_Person_Type_3,
    t2.[PAR04] AS Plan_Annual_Rate_4,
    t2.[SLMN04] AS Sales_Person_Number_4,
    t2.[SLMT04] AS Sales_Person_Type_4
FROM
    CIPNAME0 t1
LEFT JOIN 
	SCPDIVF0 t2 
ON
	t1.CUNO = t2.CUNO AND t1.DIVI = t2.DIVI




    -- SELECT DISTINCT 
--     [CUNO] AS Customer_Number,
--     [CUNM] AS Customer_Name,
--     [DIVI] AS Customer_Division,
--     CUTYP AS Customer_Type,
--     NULL AS Customer_Street_Address,
--     ZIPCD9 AS Customer_ZipCode,
--     CUCYST AS [Customer_City],
--     CUSTE AS [Customer_State],
--     CCNTRY AS [Customer_Country],
--     NULL AS [Customer_Industry_Code],
--     NULL AS [Customer_Industry_Desc],
--     CUSTST AS [Customer_Store_Number],
--     CRMGR AS [Customer_Credit_Manager],
--     NULL AS [Customer_Credit_Rep],
--     NULL AS [Customer_Sales_Rep],
--     TERMCD AS [Customer_terms_code],
--     PRCUNO AS [Parent_Customer_Number],
-- 	NULL AS [Parent_Customer_Name],
--     FLGDLI AS [is_deleted_ind],
--     DTSYS8 AS Customer_Acquired_date,
--     LOC AS Customer_Class
-- FROM
--     CIPNAME0