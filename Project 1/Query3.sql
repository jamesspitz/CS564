select count(*) from Category
group by itemId;
where COUNT ( { [ [ ALL | DISTINCT ] Item.Category ] | * } ) = 4;