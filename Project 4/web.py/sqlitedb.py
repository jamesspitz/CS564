import web

db = web.database(dbn='sqlite',
        db='AuctionBase' #TODO: add your SQLite database filename
    )

######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')

# initiates a transaction on the database
def transaction():
    return db.transaction()
# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples

# returns the current time from your database
def getTime():
    # TODO: update the query string to match
    # the correct column and table name in your database
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    return results[0].Time # TODO: update this as well to match the
                                  # column name

# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # TODO: rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Items where itemID = $itemID'
    result = query(query_string, {'itemID': item_id})
    try: return result[0]
    except IndexError: return None

def getCategoryById(item_id):
    query_string = 'select group_concat(Category,", ") as Category from Categories where ItemID = $itemID'
    result = query(query_string, {'itemID': item_id})
    try: return result[0]
    except IndexError: return None

def getBidById(item_id):
    query_string = 'select UserID as "ID of Bidder", Time as "Time of Bid", Amount as "Price of Bid" from Bids where ItemID = $itemID'
    result = query(query_string, {'itemID': item_id})
    try: return result #lil comment
    except IndexError: return None

def getWinnerById(item_id):
    query_string = 'select * from Bids where ItemID = $itemID and Amount = (select Max(Amount) from Bids where ItemID = $itemID)'
    result = query(query_string, {'itemID': item_id})
    try: return result[0]
    except IndexError: return None

def getUserById(user_id):
    query_string = 'select * from Users where UserId = $userID'
    result = query(query_string, {'userID': user_id})
    try : return result[0]
    except IndexError: return None

# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars = {}):
    return list(db.query(query_string, vars))

#####################END HELPER METHODS######(###############

#TODO: additional methods to interact with your database,
# e.g. to update the current time

def update_auction_time(time): #!!
    tran = transaction()
    try: db.update('CurrentTime', where = 'Time = $cTime', vars = { 'cTime': getTime() }, Time = time)
    except Exception as timeEx:
        tran.rollback()
        print(str(timeEx))
        raise Exception
    else: tran.commit()

#place a  bid on item by user, !!inputs were curr_item, curr_user, etc. was called new_bid
def new_bid(item, user, amount):
    tran = transaction()
    try: db.insert('Bids', UserID = user, ItemID = item, Amount = amount, Time = getTime())
    except Exception:
        tran.rollback()
        return False
    else:
        tran.commit()
        return True

def auction_search(itemID, userID, category, description, minPrice, maxPrice, status):

    if description is None: 
        description = '%%'
    else: 
        description = '%' + description + '%'
    if minPrice == '': 
        minPrice = 0
    else: 
        minPrice = int(minPrice)
    if maxPrice == '': 
        maxPrice = 999999999999999999
    else: 
        maxPrice = int(maxPrice)

    if status == 'open':
        query_string = 'select *, group_concat(category,", ") as Category from Items, Categories, CurrentTime where (Categories.ItemID = Items.ItemID) AND (IFNULL($category, "") = "" OR $category = Categories.category) AND (IFNULL($itemID, "") = "" OR $itemID = Items.ItemID) AND (IFNULL($userID, "") = "" OR $userID = Items.Seller_UserID) AND (Items.Description LIKE $description) AND (IFNULL(Items.Currently, Items.First_Bid) >= $minPrice) AND (IFNULL(Items.Currently, Items.First_Bid) <= $maxPrice) AND (Items.Started <= CurrentTime.Time AND Items.Ends >= CurrentTime.Time) group by Items.ItemID'
    if status == 'close':
        query_string = 'select *, group_concat(category,", ") as Category from Items, Categories, CurrentTime where (Categories.ItemID = Items.ItemID) AND (IFNULL($category, "") = "" OR $category = Categories.category) AND (IFNULL($itemID, "") = "" OR $itemID = Items.ItemID) AND (IFNULL($userID, "") = "" OR $userID = Items.Seller_UserID) AND (Items.Description LIKE $description) AND (IFNULL(Items.Currently, Items.First_Bid) >= $minPrice) AND (IFNULL(Items.Currently, Items.First_Bid) <= $maxPrice) AND (Items.Ends < CurrentTime.Time) group by Items.ItemID'
    if status == 'notStarted': 
        query_string = 'select *, group_concat(category,", ") as Category from Items, Categories, CurrentTime where (Categories.ItemID = Items.ItemID) AND (IFNULL($category, "") = "" OR $category = Categories.category) AND (IFNULL($itemID, "") = "" OR $itemID = Items.ItemID) AND (IFNULL($userID, "") = "" OR $userID = Items.Seller_UserID) AND (Items.Description LIKE $description) AND (IFNULL(Items.Currently, Items.First_Bid) >= $minPrice) AND (IFNULL(Items.Currently, Items.First_Bid) <= $maxPrice) AND (Items.Started > CurrentTime.Time) group by Items.ItemID'
    if status == 'all':
        query_string = 'select *, group_concat(category,", ") as Category from Items, Categories, CurrentTime where (Categories.ItemID = Items.ItemID) AND (IFNULL($category, "") = "" OR $category = Categories.category) AND (IFNULL($itemID, "") = "" OR $itemID = Items.ItemID) AND (IFNULL($userID, "") = "" OR $userID = Items.Seller_UserID) AND (Items.Description LIKE $description) AND (IFNULL(Items.Currently, Items.First_Bid) >= $minPrice) AND (IFNULL(Items.Currently, Items.First_Bid) <= $maxPrice) group by Items.ItemID'
    
    result = query(query_string, {'category': category, 'itemID': itemID, 'userID': userID, 'description': description, 'minPrice': minPrice, 'maxPrice': maxPrice})
    try: 
        return result #lilcomment
    except IndexError: 
        return None


