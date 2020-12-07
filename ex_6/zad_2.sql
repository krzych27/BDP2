--Zad2:
--a)
CREATE TABLE AdventureWorksDW2019.dbo.stg_dimemp (EmployeeKey INT, 
			FirstName NVARCHAR(50), LastName NVARCHAR(50), Title NVARCHAR(50));

INSERT INTO AdventureWorksDW2019.dbo.stg_dimemp
SELECT EmployeeKey, FirstName, LastName, Title
FROM DimEmployee
WHERE EmployeeKey BETWEEN 270 AND 275;

--b)
IF OBJECT_ID('AdventureWorksDW2019.dbo.stg_dimemp', 'u') IS NOT NULL
DROP TABLE AdventureWorksDW2019.dbo.stg_dimemp;

--c)
SELECT * INTO scd_dimemp FROM AdventureWorksDW2019.dbo.stg_dimemp WHERE 1 = 0;
ALTER TABLE scd_dimemp ADD StartDate DATETIME, EndDate DATETIME;
