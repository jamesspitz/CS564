select distinct userId from Item
where userId in (
select distict userId from Bid);