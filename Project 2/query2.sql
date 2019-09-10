select Store from TemporalData where UnemploymentRate > 10 
AND Store not in (select Store from TemporalData where FuelPrice > 4) group by store;