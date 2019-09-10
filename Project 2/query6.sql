Create view fourAtts
as
Select corr(weeklysales, temperature) as cT,
 corr(weeklysales, fuelprice) as cF,
 corr(weeklysales, cpi) as cC,
 corr(weeklysales, unemploymentrate) as cU,

Case  when corr(weeklysales, temperature)> -1 then '+'
	When corr(weeklysales, temperature) < 0 then '-' end as sT,
Case  when corr(weeklysales, fuelprice) > -1 then '+'
	When corr(weeklysales, fuelprice) < 0 then '-' end as sF,
Case  when corr(weeklysales, cpi) > -1 then '+'
	When corr(weeklysales, cpi) < 0 then '-' end as sC,
Case  when corr(weeklysales, unemploymentrate) > -1 then '+'
	When corr(weeklysales, unemploymentrate) < 0 then '-' end as sU

From temporaldata t, sales s
Where t.store = s.store and t.weekdate = s.weekdate;

Create table atts
(Attribute varchar(20),
Corr_Sign char(1),
Correlation float);


Insert into atts values('Temperature');
Insert into atts values('FuelPrice');
Insert into atts values('CPI');
Insert into atts values('UnemploymentRate');

update atts 
set corr_sign = (select sT from fourAtts), correlation = (select cT from fourAtts) 
where attribute = 'Temperature';

update atts 
set corr_sign = (select sC from fourAtts), correlation = (select cC from fourAtts) 
where attribute = 'CPI';

update atts 
set corr_sign = (select sF from fourAtts), correlation = (select cF from fourAtts) 
where attribute = 'FuelPrice';

update atts 
set corr_sign = (select sU from fourAtts), correlation = (select cU from fourAtts) 
where attribute = 'UnemploymentRate';

Select * from atts;