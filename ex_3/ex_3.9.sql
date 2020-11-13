CREATE PROCEDURE ex9(@YearsAgo AS SMALLINT) 
AS
BEGIN
SELECT * FROM AdventureWorksDW2019.dbo.FactCurrencyRate AS FCR
INNER JOIN AdventureWorksDW2019.dbo.DimCurrency AS DC
ON DC.CurrencyKey = FCR.CurrencyKey
WHERE(DATEPART(YEAR,FCR.Date) <= DATEPART(YEAR,GetDate()) - @YearsAgo
AND DATEPART(MONTH, FCR.Date) <= DATEPART(MONTH,GetDate())
AND DATEPART(DAY, FCR.Date) <= DATEPART(DAY,GetDate())
AND (DC.CurrencyAlternateKey = 'GBP' OR DC.CurrencyAlternateKey = 'EUR'));
END;
