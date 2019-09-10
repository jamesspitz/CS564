CREATE VIEW NonHolidaySales(WeeklySales)
	AS SELECT SUM(S.WeeklySales)
	FROM Sales S, Holidays H
	WHERE S.WeekDate = H.WeekDate AND H.isHoliday = 'f'
	GROUP BY S.WeekDate;

CREATE VIEW IsHolidaySales(WeeklySales)
	AS SELECT SUM(S.WeeklySales)
	FROM Sales S, Holidays H
	WHERE S.WeekDate = H.WeekDate AND H.isHoliday = 't'
	GROUP BY S.WeekDate;

select COUNT(*) FROM NonHolidaySales 
WHERE WeeklySales > (select AVG(WeeklySales) from IsHolidaySales);