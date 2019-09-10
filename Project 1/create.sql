drop table if exists Item;
drop table if exists User;
drop table if exists Category;
drop table if exists Bid;

create table Item(
itemId INTEGER PRIMARY KEY,
name TEXT NOT NULL,
currently REAL NOT NULL,
buy_price REAL,
first_bid REAL NOT NULL,
number_of_bids INTEGER NOT NULL,
started TEXT NOT NULL,
ends TEXT NOT NULL,
sellerId INTEGER NOT NULL,
description TEXT INTEGER NOT NULL
);

create table User(
userId TEXT PRIMARY KEY,
rating REAL NOT NULL,
location TEXT,
country TEXT
);

create table Category(
itemId INTEGER NOT NULL,
category TEXT NOT NULL
);

create table Bid(
itemId INTEGER  NOT NULL,
userId TEXT NOT NULL,
time TEXT NOT NULL,
amount TEXT NOT NULL
);