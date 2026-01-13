CREATE PROCEDURE [dbo].[MergeOrders]
AS
BEGIN
    -- Truncate the BulkOrders table before importing new data
    TRUNCATE TABLE [dbo].[BulkOrders];

    -- Bulk insert data from the CSV file
    BULK INSERT [dbo].[BulkOrders]
    FROM 'MergeTheseOrders.csv'
    WITH (
        DATA_SOURCE = 'MyAzureBlobStorage',
        FORMAT = 'CSV',
        FIRSTROW = 2
    );

    -- Merge data from BulkOrders into All_Orders_History
    MERGE INTO [dbo].[All_Orders_History] AS Target
    USING (
        SELECT 
            [ShipCity],
            [ShipState],
            [ShipAdr2],
            [ShipZip],
            [SrsPremiseType] as PremiseType,
            [SrsChainDescription] as Chain,
            [CusKey],
            [CusName],
            [OrderSalesPersonName] as SalesPerson,
            [Territory],
            [Brand],
            [Category],
            [FrontlinePrice],
            [ItemKey],
            [ItemName],
            [Package],
            [PackName],
            [OrderNumber],
            [SalesDate],
            [Cases],
            [Equiv_cases_4_decimal] as EquivCases,
            [Gallons],
            [NetRevenue],
            [Barrels]      
        FROM [dbo].[BulkOrders]
    ) AS Source
    ON Target.[OrderNumber] = Source.[OrderNumber] 
    AND Target.[SalesDate] = Source.[SalesDate]
    AND Target.[ItemName] = Source.[ItemName] -- Matching on ItemName
    AND Target.[Package] = Source.[Package] -- Matching on Package
    AND Target.[Cases] = Source.[Cases] -- Matching on Cases
    WHEN NOT MATCHED THEN
        INSERT (
            [ShipCity],
            [ShipState],
            [ShipAdr2],
            [ShipZip],
            [PremiseType],
            [Chain],
            [CusKey],
            [CusName],
            [SalesPerson],
            [Territory],
            [Brand],
            [Category],
            [FrontlinePrice],
            [ItemKey],
            [ItemName],
            [Package],
            [PackName],
            [OrderNumber],
            [SalesDate],
            [Cases],
            [EquivCases],
            [Gallons],
            [NetRevenue],
            [Barrels]
        )
        VALUES (
            Source.[ShipCity],
            Source.[ShipState],
            Source.[ShipAdr2],
            Source.[ShipZip],
            Source.[PremiseType],
            Source.[Chain],
            Source.[CusKey],
            Source.[CusName],
            Source.[SalesPerson],
            Source.[Territory],
            Source.[Brand],
            Source.[Category],
            Source.[FrontlinePrice],
            Source.[ItemKey],
            Source.[ItemName],
            Source.[Package],
            Source.[PackName],
            Source.[OrderNumber],
            Source.[SalesDate],
            Source.[Cases],
            Source.[EquivCases],
            Source.[Gallons],
            Source.[NetRevenue],
            Source.[Barrels]
        );
END;
