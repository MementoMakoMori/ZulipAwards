import zulip
from pymongo import MongoClient
import time
from bson.json_util import dumps

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


def bulk_get_messages_from(sender, first_anchor='oldest', batch_size=1000, limit=None):
    total = 0
    anchor = first_anchor
    limit = limit or 1e18
    while total < limit:
        request = get_messages_from(sender, anchor, num_after=batch_size)
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

# def get_all_messages(human):
#     request = {
#         "anchor": "oldest",
#         "num_before": 0,
#         "num_after": 1000,
#         "narrow": [{"operator": "sender", "operand": human},
#                    {"operator": "streams", "operand": "public"},]
#     }
#     out = []
#     while True:
#         result = zl.get_messages(request)
#         messages = result.get("messages", [])
#         if messages:
#             out.append(messages)
#         if result.get("found_newest"):
#             break
#         if result.get("code") == "RATE_LIMIT_HIT":
#             time.sleep(result.get("retry-after", 1) + 0.1)
#     return list(it.chain(*out))

# this function is for testing purposes
def get_one_post(anchor="newest", before=1, after=0, client=zulip.Client(config_file="zuliprc.txt")):
    req = {
            "anchor": anchor,
            "num_before": before,
            "num_after": after,
            "narrow": [{"operator": "streams", "operand": "public"}, ]
    }
    result = client.get_messages(req)
    message = result.get("messages", [])
    if message:
        return message
    else:
        return result

if __name__ == "__main__":
    zl = zulip.Client(config_file="zuliprc.txt")
    mclient = MongoClient()
    db = mclient["rc_mldp"]
    # no_bots = list(filter(lambda x: x['is_bot']==False, zl.get_members()['members']))
    W1_batch = list(filter(lambda x: "W1'20" in x['full_name'], zl.get_members()['members']))
    ids = list(map(lambda x: x['user_id'], W1_batch))

    for id in ids:
        bulk_get_messages_from(id)
    with open("messages.json", "w") as fl:
        cursor = db.messages.find({})
        i = db.messages.count_documents({})
        j = 0
        fl.write('[')
        # the point of this loop is to avoid writing that stupid last comma
        # which later on screws up json reads
        for document in cursor:
            fl.write(dumps(document))
            j += 1
            if j == i:
                break
            else:
                fl.write(',')
        fl.write(']')

