create view overallView
as
select store, sum(weeklysales) AS sumOfSales
	from holidays H, sales S
	where H.weekdate = S.weekdate
	AND H.isholiday = 'TRUE'
	group by store;
select store, sumOfSales
from overallView
where (sumOfSales = (select max(sumOfSales)
	from overallView)
	OR sumOfSales= (select min(sumOfSales)
	from overallView)
	);