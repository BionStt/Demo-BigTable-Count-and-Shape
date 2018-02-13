USE WideWorldImporters;
GO

--000. clean up for re-run
IF (EXISTS (SELECT index_id FROM sys.indexes
        WHERE OBJECT_ID = OBJECT_ID('WideWorldImporters.Warehouse.StockItemTransactions')
        AND name = 'IX_StockItemTransactions_TransactionOccurredWhen'))
    DROP INDEX IX_StockItemTransactions_TransactionOccurredWhen
    ON Warehouse.StockItemTransactions;

IF (NOT EXISTS (SELECT index_id FROM sys.indexes
        WHERE OBJECT_ID = OBJECT_ID('WideWorldImporters.Warehouse.StockItemTransactions')
        AND name = 'CCX_Warehouse_StockItemTransactions'))
    CREATE CLUSTERED COLUMNSTORE INDEX CCX_Warehouse_StockItemTransactions
    ON Warehouse.StockItemTransactions
        WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [USERDATA];


--A. Gathering some intel
--This is the 2nd largest table in the DB in terms of row-counts, as per the built-in SSMS report "Disk Usage by Top Tables", which doesn't use fancy features like in-memory or system-versioned.
EXEC sys.sp_spaceused 'Warehouse.StockItemTransactions'

SET STATISTICS IO ON;

--Gobble gobble (gimme all of it!)
SELECT * FROM Warehouse.StockItemTransactions
--^ that took about 2 seconds to return about 236k rows... imagine you have 10x that, do you want to wait 20 seconds just to get a row-count?  Probably not.

--Count the hard way
SELECT COUNT(1) FROM Warehouse.StockItemTransactions
--^ ok that was nearly instant.. fine, not so bad.  Right?  But what if I want the min & max date?

--Count & min/max Dates, the hard way
SELECT N = COUNT(1), MinTDate = MIN(TransactionOccurredWhen), MaxTDate = MAX(TransactionOccurredWhen) FROM Warehouse.StockItemTransactions
--^ 1/1/2013 - 5/31/2016, 236667 rows
--^ still nearly as quick, but if you look at the IO messages, you'll see over 100 "lob logical reads"

--LOB?!?  no, we don't want to read off-row data!  #StopTheInsanity
--it turns out this is actually due to the 'clustered columnstore index' that MS has cleverly applied to this table.. let's get rid of that for demo purposes.
DROP INDEX [CCX_Warehouse_StockItemTransactions] ON [Warehouse].[StockItemTransactions];

--And re-run our count/min/max query
SELECT N = COUNT(1), MinTDate = MIN(TransactionOccurredWhen), MaxTDate = MAX(TransactionOccurredWhen) FROM Warehouse.StockItemTransactions
--^ Alright, now we get the "normal" logical reads, 1900 or so.  It still "felt fast", but again, scale up to your imagination...

--What if we want a count within a given date-range?  Let's say calendar year 2015.
SELECT COUNT(1) FROM Warehouse.StockItemTransactions
WHERE TransactionOccurredWhen >= '20150101' AND TransactionOccurredWhen < '20160101'
--^ 74552 rows. feels about the same speed as above, and does about the same # of logical reads (because it's still doing a table-scan!)

SET STATISTICS IO OFF;
GO

--B. Ok, let's try an index
--the cool kids' version
EXEC master.dbo.sp_BlitzIndex 'WideWorldImporters', 'Warehouse', 'StockItemTransactions'

--the boring ol' native version
EXEC sys.sp_helpindex 'Warehouse.StockItemTransactions'

--Create an index on OrderDate (why they didn't already have one, I'll never know)
--CREATE NONCLUSTERED INDEX IX_Sales_Orders_OrderDate
--ON Sales.Orders (OrderDate);
CREATE NONCLUSTERED INDEX IX_StockItemTransactions_TransactionOccurredWhen
ON Warehouse.StockItemTransactions (TransactionOccurredWhen);

SET STATISTICS IO ON;

--Count within date-range, same way as before, but this time hoping the clustered index helps.
SELECT COUNT(1) FROM Warehouse.StockItemTransactions
WHERE TransactionOccurredWhen >= '20150101' AND TransactionOccurredWhen < '20160101'
--^ feels a bit faster than before, and does far fewer logical reads - about 200.  Cool... but we can do better.

GO

--C. The better way
--First, the overall count -- this is simple, there are system catalog views we can reference.
SELECT SchemaName = s.name, TableName = t.name, [RowCount] = SUM(p.rows)
FROM sys.partitions AS p
INNER JOIN sys.tables AS t
    ON p.[object_id] = t.[object_id]
INNER JOIN sys.schemas AS s
    ON t.[schema_id] = s.[schema_id]
WHERE p.index_id IN (0,1) -- heap or clustered index
AND s.name = N'Warehouse' AND t.name = N'StockItemTransactions'
GROUP BY s.name, t.name
ORDER BY [RowCount] DESC
--^ 236667 rows, no scanning needed!

SET STATISTICS IO OFF;
GO

--Now we can use the new index's statistics histogram to find min & max values -- or at least, get close enough!
--First we need a temp-table to store output
IF (OBJECT_ID('tempdb.dbo.#StatsHist') IS NOT NULL)
	DROP TABLE #StatsHist;

CREATE TABLE #StatsHist (
    --We add some meta-info to remember what table & index we're using
    [schema_name] sysname
    , [table_name] sysname
    , [table_id] int  --generall [object_id] but I prefer to be clear
    , [index_name] sysname
    , [index_id] int
    --Everything below here is the output of sys.dm_db_stats_histogram
    , [stats_id] int
    , [step_number] int
    , [range_high_key] sql_variant
    , [range_rows] real
    , [equal_rows] real
    , [distinct_range_rows] bigint
    , [average_range_rows] real
);

--Now we'll gather the statistics histogram from our index
INSERT INTO #StatsHist (schema_name, table_name, table_id, index_name, index_id
    , stats_id, step_number, range_high_key, range_rows, equal_rows, distinct_range_rows, average_range_rows)
SELECT s.name, t.name, t.object_id, i.name, i.index_id
    , dsh.stats_id, dsh.step_number, dsh.range_high_key, dsh.range_rows, dsh.equal_rows, dsh.distinct_range_rows, dsh.average_range_rows
FROM sys.indexes i
INNER JOIN sys.tables AS t
    ON i.[object_id] = t.[object_id]
INNER JOIN sys.schemas AS s
    ON t.[schema_id] = s.[schema_id]
CROSS APPLY sys.dm_db_stats_histogram(OBJECT_ID('Warehouse.StockItemTransactions'), i.index_id) dsh
WHERE s.name = N'Warehouse' AND t.name = N'StockItemTransactions'
AND i.name = 'IX_StockItemTransactions_TransactionOccurredWhen'
;

--Oh but wait!  The stats histogram stores the value we want to look at -- "range_high_key" -- as a variant!
--That's ok, we can add a column to our temp-table of the proper type and convert those values for our own use.
ALTER TABLE #StatsHist
ADD range_hk_proper datetime2;
GO

UPDATE #StatsHist
SET range_hk_proper = CONVERT(datetime2, range_high_key);

--Oh and guess what, we're gonna use this as our primary query predicate, so let's make it our clustered index too, yay?
CREATE CLUSTERED INDEX #CX_TMP_StatsHist
ON #StatsHist (range_hk_proper);
--^ Fun fact: You don't NEED to name it with a "#" prefix or "TMP" or anything, that's just my convention.  The Engine will know what it is & where it belongs (tempdb).

GO
SET STATISTICS IO ON;

--And now we can see some row-counts & date-ranges!
SELECT MinDate = MIN(range_hk_proper), MaxDate = MAX(range_hk_proper), [RowCount] = SUM(sh.range_rows + sh.equal_rows)
FROM #StatsHist sh
GROUP BY sh.table_name
--^ 1/1/2013 - 5/31/2016, 236667 rows, as expected.

SELECT MinDate = MIN(range_hk_proper), MaxDate = MAX(range_hk_proper), [RowCount] = SUM(sh.range_rows + sh.equal_rows)
FROM #StatsHist sh
WHERE range_hk_proper >= '20150101' AND range_hk_proper < '20160101'
GROUP BY sh.table_name
--^ 74552 rows as expected.

SET STATISTICS IO OFF;
GO

--ZZZ. clean up after
IF (EXISTS (SELECT index_id FROM sys.indexes
        WHERE OBJECT_ID = OBJECT_ID('WideWorldImporters.Warehouse.StockItemTransactions')
        AND name = 'IX_StockItemTransactions_TransactionOccurredWhen'))
    DROP INDEX IX_StockItemTransactions_TransactionOccurredWhen
    ON Warehouse.StockItemTransactions;

IF (NOT EXISTS (SELECT index_id FROM sys.indexes
        WHERE OBJECT_ID = OBJECT_ID('WideWorldImporters.Warehouse.StockItemTransactions')
        AND name = 'CCX_Warehouse_StockItemTransactions'))
    CREATE CLUSTERED COLUMNSTORE INDEX CCX_Warehouse_StockItemTransactions
    ON Warehouse.StockItemTransactions
        WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) ON [USERDATA];
