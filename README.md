# Recurse Center Zulip Awards
### What does this code do?
This code grabs RC Zulip messages from a set timeframe (the start/end of a batch) and generates fun little awards based on pretty arbitrary metrics. The goal is to have fun, so the code is relatively fragile and not necessarily 'fair' in its granting awards.

### Can I make Zulip Awards for my batch?
Yes! You will need:
* Zulip API key: on Zulip under 'Settings' -> 'Your account' you can grab an API key. 
* Files: getBatch.py, redCarpet.py, anchors.json, requirements.txt

(Actually first, `pip install -r requirements.txt` in your virtual environment)

First, edit line 149 of getBatch.py to insert the filepath to your Zulip Config file, then run getBatch.py and follow the prompts. I highly recommend having Mongo set up for this, but if you don't want to download/install anything else then you can use the SQL option.
After running getBatch, you should have two new files in your directory: messages (either json or db), and members.json. Now, just run redCarpet.py in the same directory and voilà, you have your awards printed!

Optional: anchors.json currently (April 17, 2021) includes anchors for all batches up to mini 3 '21. To update this file with more recent batch dates, you will need to run getAnchors.py. This is more involved and requires having Mongo set up on your machine and requires RC API token as command line environment variable TOK to access the calendar. (if you update anchors.json, feel free to make a pull request to this repo!)

### Why did you make this?
This project was inspired by the idea of 'paper plate awards,' which are silly little awards drawn on papers plates and handed out to club members/students/etc at the end of a season. I was also thinking about funny sport statistics, like hockey announcers giving trivia 'the last time a rookie goalie from Russia debuted in an away game while two teammates were on long-term injury was...' which is ridiculous because, what do any of those variables have to do with each other?? Nothing really, and that's what makes it fun.

### Can I make a pull request to improve the Zulip Awards?
#### Yes, please do!
Here are a few specific things you could work on, but please don't feel limited:
* adding new categories of awards like...
  * incorporating RC Calendar data for awards based on hosting/attending events
  * using Virtual RC data for awards based on time at pairing stations/time in rooms
* making code cleaner + more readable
* update anchors.py so that it has the most recent batch date info
* changing how credentials are inserted into code for ease of use
* (long term) work on a UI to make generating Zulip Awards even easier