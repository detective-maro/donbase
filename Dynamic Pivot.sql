create procedure [adf_flat_file_insert]

as

declare @fieldCount int = (
select top 1 len(recordValue) - len(replace(recordValue, '|', '')) as fieldCount
from adf_flat_file_staging s
where recordValue like '%|%' -- May not need in code if consistent
)
,@stringCount int = 1
,@fieldSelection nvarchar(500) = ''
,@tableName nvarchar(100) = 'Price' -- Need to iterate table names
,@insertStatement nvarchar(max) = '
insert into @tableName
select @fields from
(
seect top 10 -- Should be all
f.*
row_number() over(partition by recordValue order by recordValue) row_group, -- Grouped by column dynamically
recordValue

from
adf_flat_file_staging s
cross apply [dbo].[split] (substring(recordValue,0,len(recordValue)-1,''|'') f
where tableName is null
) as a

pivot
(
max(item)
for row_group
in (@fields)
) as pvt'

while @stringCount <= fieldCount
begin
	set @fieldSelection = @fieldSelection + '[' + cast(@stringCount as nvarchar) + '],'
	set @stringCount = stringCount + 1
end

set @fieldSelection = substring(@fieldSelection, 1, len(@fieldSelection) -1) -- Remove trailing comma
set @insertStatement = replace(@insertStatement, '@fields', @fieldSelection) -- N columns generically named
set @insertStatement = replace(@insertStatement, '@tableName', @tableName) -- Table name to target insert

exec (@insertStatement)
	

