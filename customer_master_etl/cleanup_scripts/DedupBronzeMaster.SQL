WITH DuplicateCheck AS (
    SELECT 
        Customer_Number,
        Customer_Name,
        Customer_Email,
        Customer_Street_Address,
        Customer_City,
        Customer_State,
        Customer_ZipCode,
        Customer_Division,
        Source_DB,
        Source_Table,
        Source_Timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY Customer_Number, Customer_Name, Customer_Email, Customer_Street_Address, 
                         Customer_City, Customer_State, Customer_ZipCode, Customer_Division
            ORDER BY Source_Timestamp DESC
        ) AS rn
    FROM [Wagner_ODS].[Customer].[Bronze_Customer_Master]
    WHERE Customer_Number IS NOT NULL
)
DELETE FROM DuplicateCheck
WHERE rn > 1;