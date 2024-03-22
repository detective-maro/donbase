-- Create a new partition function to define partition ranges
CREATE PARTITION FUNCTION TransactionDatePartitionFunction (DATE)
AS RANGE LEFT FOR VALUES ();

-- Create a new partition scheme to define where each partition will reside
CREATE PARTITION SCHEME TransactionDatePartitionScheme
AS PARTITION TransactionDatePartitionFunction
TO ([PRIMARY]);

-- Specify the filegroup on the F drive for partition storage
ALTER PARTITION SCHEME TransactionDatePartitionScheme
    NEXT USED [FG_F_PartitionData];

-- Enable partitioning on the table
ALTER TABLE YourTableName
    ADD CONSTRAINT PK_YourTableName PRIMARY KEY CLUSTERED (PrimaryKeyColumn)
    WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
    ON TransactionDatePartitionScheme(transactionDate);

-- Now, let's populate the partition function with the appropriate ranges
DECLARE @startDate DATE = 'StartYear-01-01';
DECLARE @endDate DATE = 'EndYear-01-01';
DECLARE @currentYear INT = DATEPART(YEAR, @startDate);

DECLARE @recordCount INT;

WHILE @currentYear <= DATEPART(YEAR, @endDate)
BEGIN
    SET @recordCount = (SELECT COUNT(*) FROM YourTableName WHERE YEAR(transactionDate) = @currentYear);
    
    IF @recordCount > 1000000
    BEGIN
        DECLARE @monthStart DATE = @startDate;
        DECLARE @monthEnd DATE;
        
        WHILE @monthStart < DATEADD(YEAR, 1, @startDate)
        BEGIN
            SET @monthEnd = DATEADD(MONTH, 1, @monthStart);
            
            ALTER PARTITION FUNCTION TransactionDatePartitionFunction()
                SPLIT RANGE(@monthEnd);
            
            SET @monthStart = @monthEnd;
        END;
    END
    ELSE IF @recordCount > 500000
    BEGIN
        DECLARE @quarterStart DATE = @startDate;
        DECLARE @quarterEnd DATE;
        
        WHILE @quarterStart < DATEADD(YEAR, 1, @startDate)
        BEGIN
            SET @quarterEnd = DATEADD(QUARTER, 1, @quarterStart);
            
            ALTER PARTITION FUNCTION TransactionDatePartitionFunction()
                SPLIT RANGE(@quarterEnd);
            
            SET @quarterStart = @quarterEnd;
        END;
    END
    ELSE
    BEGIN
        ALTER PARTITION FUNCTION TransactionDatePartitionFunction()
            SPLIT RANGE(@startDate);
    END;
    
    SET @startDate = DATEADD(YEAR, 1, @startDate);
    SET @currentYear = DATEPART(YEAR, @startDate);
END;
