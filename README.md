# Demo-BigTable-Count-and-Shape
Demonstrate bad &amp; good methods of counting rows in a "large" table in TSQL (MSSQL).

Requirements: SQL Server 2016 (or above), any edition, with WideWorldImporters sample database, & compatible SSMS.

Blog post: https://natethedba.wordpress.com/t-sql-tuesday-99-counting-rows-the-less-hard-way

Brief overview: using table WideWorldImporters.Warehouse.StockItemTransactions, demonstrate how to count rows,
find min & max value of key date column 'TransactionOccurredWhen', and count rows within a given date-range of that column --
all without having to scan & lock the table itself. Uses system catalog views and dynamic management function to get statistics
histogram and glean answers from there.
