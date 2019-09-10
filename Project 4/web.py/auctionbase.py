import sys; sys.path.insert(0, 'lib')
import os                            

import web
import sqlitedb
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

###########################################################################################
##########################DO NOT CHANGE ANYTHING ABOVE THIS LINE!##########################
###########################################################################################

######################BEGIN HELPER METHODS######################

# helper method to convert times from database (which will return a string)
# into datetime objects. This will allow you to compare times correctly (using
# ==, !=, <, >, etc.) instead of lexicographically as strings.

# Sample use:
# current_time = string_to_time(sqlitedb.getTime())

def string_to_time(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# helper method to render a template in the templates/ directory
#
# `template_name': name of template file to render
#
# `**context': a dictionary of variable names mapped to values
# that is passed to Jinja2's templating engine
#
# See curr_time's `GET' method for sample usage
#
# WARNING: DO NOT CHANGE THIS METHOD
def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(autoescape=True,
            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
            extensions=extensions,
            )
    jinja_env.globals.update(globals)

    web.header('Content-Type','text/html; charset=utf-8', unique=True)

    return jinja_env.get_template(template_name).render(context)

#####################END HELPER METHODS#####################

urls = ('/currtime', 'curr_time',
        '/selecttime', 'select_time',
        '/add_bid', 'place_bid',
        '/appbase', 'appbase',
        '/search', 'auction_search',
        '/items', 'selected_item',
        '/', 'auction_search')

class selected_item:

    def GET(self):
        post_params = web.input()
        itemID = post_params['id']
        item = sqlitedb.getItemById(itemID)
        categories = sqlitedb.getCategoryById(itemID)
        bids = sqlitedb.getBidById(itemID)
        ended = False
        hasBuyPrice = False
        buyPrice = ""
        winner = ""


        if item.Started <= sqlitedb.getTime() and item.Ends >= sqlitedb.getTime(): status = 'Currently Open'
        elif item.Started > sqlitedb.getTime(): status = 'Has Not Started'
        else: status = 'Closed'
        if item.Number_of_Bids == 0: noBids = True
        else: 
            noBids = False
            winner = sqlitedb.getWinnerById(itemID).UserID
        

        if item.Buy_Price is not None:
            hasBuyPrice = True
            buyPrice = item.Buy_Price
            if status == 'Closed' or float(item.Currently) >= float(item.Buy_Price):
                status = 'Closed'
                ended = True
        elif status == 'Closed':
            ended = True
        
        return render_template('items.html', id = itemID, bids = bids, Name = item.Name, Category = categories.Category, Ends = item.Ends, Started = item.Started, Number_of_Bids = item.Number_of_Bids, Seller = item.Seller_UserID, Description = item.Description, Currently = item.Currently, noBids = noBids, ended = ended, Status = status, Winner = winner, buyPrice = buyPrice, hasBuyPrice = hasBuyPrice)

class curr_time:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        current_time = sqlitedb.getTime()
        return render_template('curr_time.html', time = current_time)


class select_time:
    # Another GET request, this time to the URL '/selecttime'
    def GET(self):
        return render_template('select_time.html')

    # A POST request
    #
    # You can fetch the parameters passed to the URL
    # by calling `web.input()' for **both** POST requests
    # and GET requests
    def POST(self):
        post_params = web.input()
        MM = post_params['MM']
        dd = post_params['dd']
        yyyy = post_params['yyyy']
        HH = post_params['HH']
        mm = post_params['mm']
        ss = post_params['ss']
        enter_name = post_params['entername']

        selected_time = '%s-%s-%s %s:%s:%s' % (yyyy, MM, dd, HH, mm, ss)
        update_message = '(User: %s. Previous time: %s.)' % (enter_name, selected_time)
        try: sqlitedb.update_auction_time(selected_time)
        except Exception as timeEx: 
            update_message = 'Invalid time value, '
            print(str(timeEx))
        return render_template('select_time.html', message = update_message)



class place_bid:

    def GET(self):
        return render_template('add_bid.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        userID = post_params['userID']
        Amount = post_params['price']

        # Valid search 
        if itemID == '' or userID == '' or Amount == '':
            return render_template('add_bid.html', message = 'Error: check valid ItemID, UserID, and Amount')
        else:
            curr_item = sqlitedb.getItemById(itemID)
            curr_user = sqlitedb.getUserById(userID)
        # Valid item in database
        if curr_item is None:
            return render_template('add_bid.html', message = 'Error: invalid ItemID')
        # Valid user
        elif curr_user is None:
            return render_template('add_bid.html', message = 'Error: invalid UserID')
        # User not bidding on own item
        elif curr_user.UserID == curr_item.Seller_UserID:
            return render_template('add_bid.html', message = 'Error: User cannot bid on own items')
        # Auction ongoing
        elif string_to_time(sqlitedb.getTime()) >= string_to_time(curr_item.Ends):
            return render_template('add_bid.html', message = 'Error: auction ended')
        # Auction Started
        elif string_to_time(sqlitedb.getTime()) < string_to_time(curr_item.Started):
            return render_template('add_bid.html', message = 'Errr: auction yet to start')
        # Valid bid amount
        elif float(Amount) < 0 or float(Amount) <= float(curr_item.First_Bid) or float(Amount) <= float(curr_item.Currently):
            return render_template('add_bid.html', message = 'Error: invalid amount')
        # Valid buy price
        if curr_item.Buy_Price is not None:
            #close auction if buy price met
            if float(Amount) >= float(curr_item.Buy_Price):
                successful_purchase = 'WOOT! You now own item: %s at the low low price of: %s.' % (curr_item.Name,Amount)
                return render_template('add_bid.html', message = successful_purchase, add_result = sqlitedb.new_bid(itemID,userID,Amount))
        #Verified no errors exist, place a new bid on the item for the amount specified
        successful_bid = 'You have bid on the amazing: %s at the low low price of: %s.' % (curr_item.Name,Amount)
        return render_template('add_bid.html', message = successful_bid, add_result = sqlitedb.new_bid(itemID,userID,Amount))


class auction_search:

    def GET(self):
        return render_template('search.html')

    def POST(self):
        post_params = web.input()
        itemID = post_params['itemID']
        userID = post_params['userID']
        category = post_params['category']
        description = post_params['description']
        minPrice = post_params['minPrice']
        maxPrice = post_params['maxPrice']
        status = post_params['status']

        # Valid input added
        if itemID == '' and userID == '' and category == '' and description == '' and minPrice == '' and maxPrice == '':
            return render_template('search.html', message = 'Error: no search criteria')
        else:
            val = sqlitedb.auction_search(itemID, userID, category, description, minPrice, maxPrice, status)
            return render_template('search.html', search_result = val)

###########################################################################################
##########################DO NOT CHANGE ANYTHING BELOW THIS LINE!##########################
###########################################################################################

if __name__ == '__main__':
    web.internalerror = web.debugerror
    app = web.application(urls, globals())
    app.add_processor(web.loadhook(sqlitedb.enforceForeignKey))
    app.run()