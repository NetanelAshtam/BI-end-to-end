
USE Priority


--- ����� ��.��.�� �� ����� ���� ��� ������� ����������---
GO
EXEC sys.sp_cdc_enable_db


EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo'
  , @source_name = N'Categories'
  , @role_name = NULL
  , @capture_instance = NULL
  , @supports_net_changes = 1
  , @captured_column_list = N'ProductCategoryKey, CategoryName '
  , @filegroup_name = N'PRIMARY';

/* ����� ���� ���� ��� ��.�.� �������� ���.��.��, �� ���� ���� ���� ������ �� ������ ������������ ������� ��� ��.�.� ������
*/

CREATE TABLE [util].[HWM] (
	captureInstance SYSNAME NOT NULL PRIMARY KEY,
	maxLSN NVARCHAR(42) NOT NULL,
	numRecords BIGINT NOT NULL
)

---����� ������ �� ���� �����--

INSERT INTO util.HWM (captureInstance, maxLSN, numRecords)
SELECT 'staging_CategoriesDetailSource', UPPER(sys.fn_varbintohexstr(MAX(__$start_lsn))), 0
FROM [cdc].[dbo_Categories_CT]

select * from util.HWM
select * from [cdc].[dbo_Categories_CT]

----����� ������ �� ���� ����� ����� �������(��� ��� ���� ������ ��������)..�
insert into DWH.dbo.MrrCategories1
select * from priority.dbo.Categories

---����� ���� ����� ������ ����� ���.��.�� ����� ������������--

CREATE VIEW staging.vwCategoriesDetailCDC 
AS

 SELECT 
 __$start_lsn 
 ,ProductCategoryKey
 ,CategoryName
 ,OperationStatus= case
 when __$operation=1 then 'delete'
 when __$operation=2 then 'insert'
 when  __$operation=3 then 'update before'
 else 'update after'
 end
 
 FROM [cdc].[dbo_Categories_CT]
GO

/*����� �������� ����� �� ����� ����������� ����� ������� ������ ��� �� ���������� ������� ����� ��������
����� �� ���� ������ ��������
*/
CREATE PROCEDURE pIncrementalLoad
AS
DECLARE @PreviousMaxLSN BINARY(10);
DECLARE @NewMaxLSN BINARY(10);
DECLARE @NumRecords INT;
DECLARE @CaptureInstance SYSNAME = 'staging_CategoriesDetailSource';

--���� �� ��.�.� ����� �����
SELECT @PreviousMaxLSN = CONVERT(BINARY(10), maxLSN, 1)
FROM util.HWM
WHERE captureInstance = @CaptureInstance;
--���� �� ��.�.� ����� ����
SELECT @NewMaxLSN = MAX(__$start_LSN) FROM staging.vwCategoriesDetailCDC ;
IF @NewMaxLSN IS NULL OR @NewMaxLSN = @PreviousMaxLSN 
BEGIN
	
	Print 'Incremental load procedure returned without making any changes.'
	
END;
--����� �� �� ������� ������ �������� ����� ����� ����� �������--
INSERT INTO Dwh.[dbo].[MrrCategories] (ProductCategoryKey,CategoryName,OperationStatus)
SELECT ProductCategoryKey,CategoryName,OperationStatus
FROM staging.vwCategoriesDetailCDC 
WHERE __$start_LSN > @PreviousMaxLSN
AND __$start_LSN <= @NewMaxLSN
SET @NumRecords = @@RowCount;
--����� �� ���� HHN ���� ��.�.� ���� ������
UPDATE util.HWM
SET maxLSN = UPPER(sys.fn_varbintohexstr(@NewMaxLSN)),
	numRecords = @NumRecords
WHERE captureInstance = @CaptureInstance;
PRINT 'There were ' + CAST(@Numrecords AS VARCHAR) + ' records inserted in this incremental load.'
GO


-----------------------------  �������� ��.��.�� ��� 2-------------------------------------------------------
create proc SDC_Categories as 
DECLARE @Yesterday INT = (YEAR(DATEADD(dd,-1,GETDATE())) * 10000) + (MONTH(DATEADD(dd,-1,GETDATE())) * 100) + DAY(DATEADD(dd,-1,GETDATE()))
DECLARE @Today INT = (YEAR(GETDATE()) * 10000) + (MONTH(GETDATE()) * 100) + DAY(GETDATE())
-- Outer insert - the updated records are added to the SCD2 table
INSERT INTO dwh.dbo.DimCategories (ProductCategoryKey,CategoryName, ValidFrom, IsCurrent)
SELECT ProductCategoryKey, CategoryName, @Today, 1
FROM
(

MERGE INTO dbo.DimCategories AS T
USING [Priority].dbo.Categories AS S
ON (S.ProductCategoryKey = T.ProductCategoryKey)
-- ����� ������
WHEN NOT MATCHED THEN 
INSERT (ProductCategoryKey,CategoryName , ValidFrom, IsCurrent)
VALUES (s.ProductCategoryKey,s.CategoryName , @Today, 1)
-- ����� ������
WHEN MATCHED 
AND IsCurrent = 1
AND (
 ISNULL(t.ProductCategoryKey,'') <> ISNULL(s.ProductCategoryKey,'') 
 OR ISNULL(t.CategoryName,'') <> ISNULL(s.CategoryName,'') 

 )
-- ����� �����
THEN UPDATE 
SET t.IsCurrent = 0, t.ValidTo = @Yesterday
OUTPUT s.ProductCategoryKey,s.CategoryName , $Action AS MergeAction
) AS MRG
WHERE MRG.MergeAction = 'UPDATE'


exec  SDC_Categories 

select * from DimCategories
select * from MrrCategories





