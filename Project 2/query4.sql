CREATE VIEW MonthSalesByType(months, type, weeklysales) 
	AS SELECT EXTRACT(MONTH FROM WeekDate) as months, Type, WeeklySales 
	FROM Sales SA, Stores ST 
	WHERE SA.Store = ST.Store;

CREATE VIEW TotalSalesByType (TotalSales, Type) 
	as select SUM(weeklysales) as totalsales, type )
	from MonthSalesByType 
	group by type;

CREATE VIEW MonthSalesByTypeTotal (months, type, totalweeklysales)
	AS SELECT months, type, sum(weeklysales) as sum
from MonthSalesByType
group by type, months;

select months, M.type, totalweeklysales as sum, (totalweeklysales/totalsales)*100 as "%contribution" 
from MonthSalesByTypeTotal M, TotalSalesByType S
where M.type = S.type
order by M.type, months;
