select itemId, currently from Item
where currently = (SELECT max(currently) from Item;