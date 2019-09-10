Create view contSales
as 
Select store, dept, extract(month from weekdate) as month, extract(year from weekdate) as year,sum(weeklysales)
From sales
Group by store,dept,month, year;

select store
from stores
except
select distinct store
from contSales c
where exists(select year from contSales where year = '2010' AND store = c.store)
 	AND exists(select year from contSales where year = '2011' AND store = c.store)
AND exists(select year from contSales where year = '2012' AND store = c.store)
AND c.sum < 1;