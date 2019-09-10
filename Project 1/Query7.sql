select COUNT(distinct category from Category
where itemId in (
select itemId from Item where currently >= 100;
));