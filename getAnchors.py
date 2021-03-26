import zulip
import pymongo
import time
import requests
import datetime as dt
import json

"""
IMPORTANT
running this script will collect all public RC Zulip messages minus bots
if you are running this for the first time, it will take at least 20 min
this script requires a running Mongo server (mongod daemon on unix)
"""


def get_messages_from(sender, anchor='oldest', num_after=1000):
    """Get up to num_after messages from sender to public streams, starting with anchor."""
    return {
        "anchor": anchor,
        "num_before": 0,
        "num_after": num_after,
        "narrow": [
            {"operator": "sender", "operand": sender},
            {"operator": "streams", "operand": "public"},
        ]
    }


def bulk_get_messages_from(sender, first_anchor='oldest', chunk_size=1000, limit=None):
    total = 0
    anchor = first_anchor
    limit = limit or 1e18
    while total < limit:
        request = get_messages_from(sender, anchor, num_after=chunk_size)
        result = zl.get_messages(request)
        messages = result.get("messages", [])
        if messages:
            assert all(m["sender_id"] == sender for m in messages)
            db.messages.insert_many(messages)
            total += len(messages)
        if result.get("found_newest"):
            break
        if result.get("code") == "RATE_LIMIT_HIT":
            time.sleep(result.get("retry-after", 1) + 0.1)
        else:
            anchor = max(m["id"] for m in messages) + 1
    return "OK"


def parse_batch(name):
    split = str.split(name)
    if len(split) == 2:
        num = ""
        year = split[1][-2:]
    else:
        num = split[1][0]
        year = split[2][-2:]
    season = ""
    if split[0] == "Spring":
        season = "SP"
    if split[0] == "Summer":
        season = "S"
    if split[0] == "Fall":
        season = "F"
    if split[0] == "Winter":
        season = "W"
    if split[0] == "Mini":
        season = "m"
    new = f"{season}{num}'{year}"
    return new


def collect_dates(batches):
    batches['tag'] = parse_batch(batches['name'])
    batches['start_date'] = dt.datetime.timestamp(dt.datetime.strptime(batches['start_date'], "%Y-%m-%d"))
    batches['end_date'] = dt.datetime.timestamp(dt.datetime.combine(dt.datetime.date(dt.datetime.strptime(
        batches['end_date'], "%Y-%m-%d")), dt.time(23, 59, 59)))
    return batches


def remove_future(batches):
    earliest = db.messages.find({}, {'timestamp': 1}).sort('timestamp', pymongo.ASCENDING)[0]['timestamp']
    if batches['start_date'] < earliest:
        return False
    # if batches['end_date'] > dt.datetime.timestamp(dt.datetime.now()):
    #     return False
    else:
        return True


if __name__ == "__main__":
    # import os
    # zl = zulip.Client(config_file=os.getenv('Z_RC'))
    zl = zulip.Client(config_file="~/PycharmProjects/ZULIP_CONFIG")
    mclient = pymongo.MongoClient()
    db = mclient["all_rc"]
    no_bots = list(filter(lambda x: x['is_bot'] is False, zl.get_members()['members']))
    ids = list(map(lambda x: x['user_id'], no_bots))
    if db.messages.count_documents({}) > 0:
        latest = db.messages.find({}).sort('timestamp', pymongo.DESCENDING)[0]['id'] + 1
        for human in ids:
            bulk_get_messages_from(human, first_anchor=latest)
    else:
        for human in ids:
            bulk_get_messages_from(human)
    if 'timestamp_1' not in db.messages.index_information():
        db.messages.create_index([('timestamp', pymongo.DESCENDING)])
    # grab batch dates from the RC calendar
    # import os
    # tok = os.getenv('TOKEN')
    import keyring
    tok = keyring.get_password('summon', 'RC_API_TOKEN')
    batch_info = requests.get(url=f"https://www.recurse.com/api/v1/batches?access_token={tok}")
    form_info = list(map(collect_dates, batch_info.json()))
    form_info = list(filter(remove_future, form_info))
    # use 'start_date' and 'end_date' to  make batch anchors
    anchors = {}
    for batch in form_info:
        anchors.update({batch['tag']: {}})
        time_frame = list(db.messages.find({
            "$and": [{
                "timestamp": {"$gte": batch['start_date']}
            }, {
                "timestamp": {"$lte": batch['end_date']}
            }]
        }, {'id': 1}).sort("timestamp", pymongo.ASCENDING))
        anchors[batch['tag']]['first'] = time_frame[0]['id']
        anchors[batch['tag']]['last'] = time_frame[len(time_frame)-1]['id']
    with open('anchors.json', 'w') as fl:
        fl.write(json.dumps(anchors))
        fl.close()
    mclient.close()
