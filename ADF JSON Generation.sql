create procedure ADF_JSON_Generation
@singleTable nvarchar(500)

as 

BEGIN

set nocount ON

if not exists(select top 1* from sys.tables where name = 'ADF_Mapping'
	BEGIN
create table ADF_Mapping( [ADFTypeMappingID] int, [ADFTypeDataType] varchar(20), [SQLServerDataType] varchar(20) )	
insert into ADF_Mapping ( [ADFTypeMappingID], [ADFTypeDataType], [SQLServerDataType])	

VALUES
( 1, 'Int64', 'BIGINT' ), 
( 2, 'Byte array', 'BINARY' ), 
( 3, 'Boolean', 'BIT' ), 
( 4, 'String', 'CHAR' ), 
( 5, 'DateTime', 'DATE' ), 
( 6, 'DateTime', 'DATETIME' ), 
( 7, 'DateTime', 'DATETIME2' ), 
( 8, 'DateTimeOffset', 'DATETIMEOFFSET' ), 
( 9, 'Decimal', 'DECIMAL' ), 
( 10, 'Double', 'FLOAT' ), 
( 11, 'Byte array', 'IMAGE' ), 
( 12, 'Int32', 'INT' ), 
( 13, 'Decimal', 'MONEY' ), 
( 14, 'String', 'NCHAR' ), 
( 15, 'String', 'NTEXT' ), 
( 16, 'Decimal', 'NUMERIC' ), 
( 17, 'String', 'NVARCHAR' ), 
( 18, 'Single', 'REAL' ), 
( 19, 'Byte array', 'ROWVERSION' ), 
( 20, 'DateTime', 'SMALLDATETIME' ), 
( 21, 'Int16', 'SMALLINT' ), 
( 22, 'Decimal', 'SMALLMONEY' ), 
( 23, 'Byte array', 'SQL_VARIANT' ), 
( 24, 'String', 'TEXT' ), 
( 25, 'DateTime', 'TIME' ), 
( 26, 'String', 'TIMESTAMP' ), 
( 27, 'Int16', 'TINYINT' ), 
( 28, 'GUID', 'UNIQUEIDENTIFIER' ), 
( 29, 'Byte array', 'VARBINARY' ), 
( 30, 'String', 'VARCHAR' ), 
( 31, 'String', 'XML' ), 
( 32, 'String', 'JSON' )
end

if not exists (select top 1* from sys.tables where name = 'ADF_Config'
BEGIN
Create table ADF_Config (
fileTemplateID int identity (1,1) not null,
fileTemplateCode nvarchar(200) null,
fileTemplateSource nvarchar(200) null,
JSONMapping nvarchar(max) null,
SinkDestinationName nvarchar(200) null,
createdBy nvarchar(200) null,
createdDate datetime NULL,
isCustom bit default 0,
active bit default 0,
keyID nvarchar(50) null,
maxID int default 0
constraint [PK_ADF_Config_ID] primary key nonclustered
(fileTemplateID asc)
-- Generic stats crap
)
end

declare @tables table (tableName nvarchar(500))

declare @jsonSQL nvarchar(max),
@schema nvarchar(500) = 'dbo',
@tableName nvarchar(500)

if @singleTable is not null
	BEGIN
	Insert into @tables
	selet @singleTable
	end
if singleTable is NULL
		begin
		insert into @tables 
		select distinct name
		from sys.tables 
		where name != 'Log'
		end
		
declare @json_loop cursor 
FOR
select * from @tables 

open json_loop

fetch Next 
from json_loop
into @tableName
while @@fetch_status = 0
BEGIN
declare @S@QL nvarchar(max) = '

insert into ADF_Config (
fileTemplateCode,
fileTemplateSource,
JSONMapping,
SinkDestinationName,
createdBy,
createdDate,
isCustom,
active,
keyID,
maxID
)		

select
''@tableName'',
''@tableName'',
''{"type": "TabularTranslator", "mappings": '' + 
(
    SELECT
         ''source.path''  = IIF(c.[name] = ''Guid'',''GUID_regel'',c.[name])
        ,''source.type''  = m.ADFTypeDataType
        ,''sink.name''    = c.[name]
        ,''sink.type''    = m.ADFTypeDataType
    FROM sys.tables                 t
    JOIN sys.schemas                s ON s.schema_id        = t.schema_id
    JOIN sys.all_columns            c ON c.object_id        = t.object_id
    JOIN sys.types                  y ON c.system_type_id   = y.system_type_id
                                        AND c.user_type_id  = y.user_type_id
    JOIN etl.ADF_DataTypeMapping    m ON y.[name]           = m.SQLServerDataType
    WHERE   1 = 1
        AND t.[name] = @tableName
        AND s.[name] = ''@schema''
        AND c.[name] <> ''SRC_TIMESTAMP''
    ORDER BY c.column_id
    FOR JSON PATH
),
''@tableName'',
''ADF_Pipeline'',
getdate(),
1,
(select c.name
		from sys.tables t
		join sys.columns c on c.object_id = t.object_id
 WHERE
column_id = 1
and t.name = ''@tableName''),
0)'

set @sql = replace(@sql, '@tableName', @tableName)
set @sql = replace(@sql, '@schema', @schema)

exec(@sql)

fetch NEXT
from json_loop
into @tableName
end

close json_loop

deallocate json_loop
end