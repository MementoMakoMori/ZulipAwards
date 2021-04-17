import zulip
import time
import json
from bson.json_util import dumps
from functools import reduce


# this function removes bots from the collected messages; limits streams only to RC
def message_filter(message_list):
    global bot_ids
    bots = list(filter(lambda x: x['sender_id'] in bot_ids, message_list))
    if bots:
        for i in bots:
            message_list.remove(i)
    non_rc = list(filter(lambda x: x['sender_realm_str'] == 'zulipcore', message_list))
    if non_rc:
        for i in non_rc:
            message_list.remove(i)
    return message_list


# retrieve messages by batch timeframe and enter them into a MongoDB
def mongo_batch_only(start_anchor, end_anchor, chunk_size=500, limit=None):
    total = 0
    anchor = start_anchor
    limit = limit or 1e18
    while total < limit:
        req = {
            "anchor": anchor,
            "num_before": 0,
            "num_after": chunk_size,
            "narrow": [
                {"operator": "streams", "operand": "public"}
            ]
        }
        result = zl.get_messages(req)
        messages = result.get("messages", [])
        if messages:
            messages = message_filter(messages)
            cut_off = list(filter(lambda x: x['id'] > end_anchor, messages))
            if cut_off:
                for over in cut_off:
                    messages.remove(over)
                if messages:
                    db.messages.insert_many(messages)
                break
            else:
                db.messages.insert_many(messages)
                total += len(messages)
                anchor = max(m["id"] for m in messages) + 1
        if result.get("found_newest"):
            break
        if result.get("code") == "RATE_LIMIT_HIT":
            time.sleep(result.get("retry-after", 1) + 0.1)
    return "OK"


# create SQLite table to insert messages into
def sql_create():
    sql_command = '''CREATE TABLE messages
    (id int not null,
    sender_id   int not null,
    content text    not null,
    recipient_id    int,
    timestamp_   timestamp   not null,
    client  text,
    subject text,
    topic_links text,
    is_me_message   boolean,
    reactions  text,
    submessages text,
    flags   text,
    sender_full_name    text    not null,
    sender_email    text,
    sender_realm_str    text,
    display_recipient   text    not null,
    type    text,
    stream_id   int not null,
    avatar_url  text,
    content_type    text);'''
    return sql_command


# retrieve messages by batch timeframe and enter them into SQLite table
def sql_batch_only(start_anchor, end_anchor, chunk_size=500, limit=None):
    total = 0
    anchor = start_anchor
    limit = limit or 1e18
    while total < limit:
        req = {
            "anchor": anchor,
            "num_before": 0,
            "num_after": chunk_size,
            "narrow": [
                {"operator": "streams", "operand": "public"}
            ]
        }
        result = zl.get_messages(req)
        messages = result.get("messages", [])
        if messages:
            messages = message_filter(messages)
            cut_off = list(filter(lambda x: x['id'] > end_anchor, messages))
            if cut_off:
                for over in cut_off:
                    messages.remove(over)
                c.executemany('''INSERT INTO messages VALUES (?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', map(clean_for_sql, messages))
                break
            else:
                c.executemany('''INSERT INTO messages VALUES (?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', map(clean_for_sql, messages))
                total += len(messages)
                anchor = max(m["id"] for m in messages) + 1
        if result.get("found_newest"):
            break
        if result.get("code") == "RATE_LIMIT_HIT":
            time.sleep(result.get("retry-after", 1) + 0.1)
    return "OK"


# check input to use Mongo or SQL
def choose_checker(text):
    if text.isalpha():
        if len(text) == 1:
            if 'y' in text.lower():
                return True
    return False


# SQL table cannot handle different # of fields, so remove last_edit_timestamp
# which only appears on messages that were edited
def clean_for_sql(message: dict):
    if 'last_edit_timestamp' in message.keys():
        del message['last_edit_timestamp']
    for item in message:
        if type(message[item]) is list or type(message[item]) is dict:
            message[item] = str(message[item])
    return list(message.values())


# helper to get bot_ids for removal
def reduce_bots(acc, nxt):
    acc.append(nxt['user_id'])
    return acc


if __name__ == "__main__":
    # here is where you need to change code to connect to your Zulip credentials
    zl = zulip.Client(config_file="INSERT FILE PATH HERE")
    # if you want to make your own anchors.json file, run getAnchors.py
    anchors = json.load(open("anchors.json", "r"))
    batch = input("Which batch are you selecting?\nPlease use the format InitialNumber'Yr, like this: SP1'18\n")
    while batch not in anchors.keys():
        batch = input("Please format your input like W2'19, or m1'20\n")
    batch_start = anchors[batch]['first']
    batch_end = anchors[batch]['last']
    batch_members = list(filter(lambda x: batch in x['full_name'], zl.get_members()['members']))
    bot_members = list(filter(lambda x: x['is_bot'] is True, zl.get_members()['members']))
    # members.json is a list of people in batch & will be used by redCarpet.py
    with open("members.json", "w") as fl:
        fl.write(json.dumps(batch_members))
        fl.close()
    bot_ids = reduce(reduce_bots, bot_members, [])
    choose = input("Are you using Mongo for your data? (recommended)\ny/[n]")
    if choose_checker(choose):
        import pymongo

        mclient = pymongo.MongoClient()
        db = mclient[batch]
        mongo_batch_only(start_anchor=batch_start, end_anchor=batch_end)
        with open("messages.json", "w") as fl:
            cursor = db.messages.find({})
            last = db.messages.count_documents({})
            i = 0
            fl.write('[')
            # the point of this loop is to avoid writing that stupid last comma
            # which later on screws up json reads
            for document in cursor:
                fl.write(dumps(document))
                i += 1
                if i == last:
                    break
                else:
                    fl.write(',')
            fl.write(']')
            fl.close()
    else:
        import sqlite3

        db = sqlite3.connect("messages.db")
        c = db.cursor()
        c.execute(sql_create())
        sql_batch_only(start_anchor=batch_start, end_anchor=batch_end)
        db.commit()
        db.close()
    print("messages write complete")
