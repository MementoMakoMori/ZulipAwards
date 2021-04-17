import pandas as pd
from functools import reduce
from itertools import chain
import re
from nltk.tokenize import RegexpTokenizer
import os
import datetime as dt

"""
HELPER FUNCTIONS
"""

# this tokenizer will be used to split words, omitting punctuation
tk = RegexpTokenizer(r'\w+')


def find_files():
    files = os.listdir()
    if 'members.json' in files:
        if 'messages.json' in files:
            return {'members': 'members.json', 'messages': 'messages.json', 'type': 'json'}
        elif 'messages.db' in files:
            return {'members': 'members.json', 'messages': 'messages.db', 'type': 'sql'}
    else:
        print("Error: No members/messages files found in this directory. Run getBatch.py to create them.")


def flat_reducer(acc, nxt):
    acc.update(nxt)
    return acc


def count_reducer(acc, nxt):
    acc[nxt] = acc.get(nxt, 0) + 1
    return acc


def remove_code(text: str) -> str:
    text = re.sub("<[^>]+>", "", text)
    text = re.sub("^```.*```$", "", text)
    text = re.sub("\\n", "", text)
    return text


"""
AWARDS START HERE
each function is a category of award, some categories have multiple awards with their own print statements
to remove an award, comment the print statement
to remove an entire category, comment the function call in __main__
"""


def most_links(df):

    def get_rate(name):
        if counts.get(name) == 0 or counts.get(name) is None:
            return {name: 0}
        else:
            return {name: counts.get(name) / totals.get(name)}

    df = df.drop(index=df[df.content.str.contains("img src")].index)
    totals = list(map(lambda x: {x: len(df[(df['sender_full_name'] == x)])}, names))
    totals = reduce(flat_reducer, totals)
    links = df.loc[df['content'].str.contains('<a href')]
    counts = reduce(count_reducer, links['sender_full_name'], {})
    most = max(counts, key=counts.get)
    rates = list(map(get_rate, names))
    rates = reduce(flat_reducer, rates)
    m_rate = max(rates, key=rates.get)
    print("""
    RC Librarians
    ---------------
    Most links in posts: {}, {}
    Most links per post: {}, {}%
    (stats for non-image links)
    """.format(most, counts.get(most), m_rate, rates.get(m_rate)))


# this award feels too much like a popularity contest so I don't run it after the first batch W1'20
# don't worry Julia, you'll always be RC Prom Queen in my heart
def receive_emojis(df):
    def get_react(name):
        mess = chain(*list(filter(lambda x: len(x) > 0, list(df[(df['sender_full_name'] == name)]['reactions']))))
        return {name: len(list(mess))}

    each = list(map(get_react, names))
    one_dict = reduce(flat_reducer, each)
    most = max(one_dict, key=one_dict.get)
    print("""
    Can't Help But Smile
    --------------------
    Received most reactions on posts: {}, {}
    """.format(most, one_dict.get(most)))


def most_days(df):
    def check_mapper(name):
        only_checkin = df[(df['stream_id'] == checkin_stream)]
        times = only_checkin[(only_checkin['sender_full_name'] == name)]['timestamp'].dt.strftime('%Y/%m/%d')
        return {name: len(list(times.unique()))}

    def time_mapper(name):
        times = df[(df['sender_full_name'] == name)]['timestamp'].dt.strftime('%Y/%m/%d')
        return {name: len(list(times.unique()))}

    checkin_stream = 18961
    checks = reduce(flat_reducer, map(check_mapper, names))
    tms = list(map(time_mapper, names))
    res = reduce(flat_reducer, tms)
    most = max(res, key=res.get)
    m_checks = max(checks, key=checks.get)
    print("""
    Consistency is Key
    ------------------
    Most posts on different days: {}, {}
    Most checkins on different days: {}, {}
    """.format(most, res.get(most), m_checks, checks.get(m_checks)))


def give_emojis(df):
    def fav_emoji(name):
        mojis = list(filter(lambda x: x['user']['full_name'] == name, batch_reacts))
        sum_moji = {}
        for react in mojis:
            k = react['emoji_name']
            sum_moji[k] = sum_moji.get(k, 0) + 1
        return max(sum_moji, key=sum_moji.get)

    only_reacts = list(chain(*filter(lambda x: len(x) > 0, df['reactions'])))
    batch_reacts = list(filter(lambda x: x['user']['full_name'] in names, only_reacts))
    totals = {}
    for emoji in batch_reacts:
        totals[emoji['user']['full_name']] = totals.get(emoji['user']['full_name'], 0) + 1
    if len(totals) != 0:
        most = max(totals, key=totals.get)
        fav = fav_emoji(most)
        print("""
    Enthusiastic!
    -------------
    Gave most reactions: {}, {}
    Their favorite emoji: {}
    """.format(most, totals.get(most), fav))


# not as much of a mess as it used to be
def short_long(df):
    def get_messages(name):
        mess = list(df[(df['sender_full_name'] == name)]['content'])
        mess = map(remove_code, mess)
        len_l = 0
        len_s = 1000
        longest = ""
        shortest = ""
        for m in mess:
            m2 = tk.tokenize(m)
            if len(m2) > len_l:
                len_l = len(m2)
                longest = m
            if 0 < len(m2) < len_s:
                len_s = len(m2)
                shortest = m
        return {name: {"len_l": len_l, "len_s": len_s, "long": longest, "short": shortest, "nchar": len(shortest)}}

    alls = reduce(flat_reducer, map(get_messages, names))
    alldf = pd.DataFrame(data=alls.values(), columns=['len_l', 'len_s', 'long', 'short', 'nchar'], index=names)
    m_long = max(alldf['len_l'])
    l_place = alldf[alldf['len_l'] == m_long]

    print("""
    Best-Selling Novelist
    ---------------------
    Longest single post by words: {}, {},
     \"{}...\"

    """.format(m_long, l_place.index[0], l_place['long'].values[0][:175], ))
    m_short = min(alldf['len_s'])
    match_len = alldf.loc[alldf['len_s'] == m_short]
    match_len = match_len.sort_values(by='nchar')
    if len(match_len) == 1:
        s_place = alldf[alldf['len_s'] == m_short]
        print("""      
    Succinct
    --------
    Shortest post by words: {}, {}, \"{}\"
    """.format(m_short, s_place.index[0], s_place['short'].values[0]))
    elif len(match_len) > 1:
        match_char = match_len.loc[match_len['nchar'] == min(match_len.nchar)]
        if len(match_char) == 1:
            print("""
    Succinct
    -------
    Shortest post by words: {}, \"{}\"
    """.format(match_char.index[0], match_char['short'].values[0]))
        else:
            print("""
    Succinct
    --------
    Shortest post by words:
    It's a tie!
        """)
            l = match_len.iloc[0]['nchar']
            i = 0
            while match_len.iloc[i]['nchar'] == l:
                print("""
    {}""".format(" ".join([match_len.index[i], ":", match_len.iloc[i]['short']])))
                i += 1


# this award and the next are often omitted
def long_messages(df):
    def get_messages(name):
        mess = list(df[(df['sender_full_name'] == name)]['content'])
        mess = list(map(remove_code, mess))
        return {name: mess}

    def find_mean_char(texts) -> float:
        lens = list(map(lambda x: len(x), texts))
        return sum(lens) / len(texts)

    def find_mean_word(texts) -> float:
        splits = list(map(lambda x: len(tk.tokenize(x)), texts))
        return sum(splits) / len(texts)

    def sum_all_words(name):
        posts = list(map(lambda x: len(tk.tokenize(x)), name_text.get(name)))
        return {name: sum(posts)}

    name_text = reduce(flat_reducer, map(get_messages, names))
    sum_words = reduce(flat_reducer, map(sum_all_words, names))
    by_char = {}
    by_word = {}
    for name in name_text.keys():
        by_char[name] = find_mean_char(name_text.get(name))
        by_word[name] = find_mean_word(name_text.get(name))
    max_char = max(by_char, key=by_char.get)
    max_word = max(by_word, key=by_word.get)
    max_sum = max(sum_words, key=sum_words.get)
    print("""
    Verbose
    --------
    Longest average # characters: {}, {}
    Longest average # words: {}, {}
    Most words posted total: {}, {}
    """.format(max_char, by_char.get(max_char), max_word, by_word.get(max_word), max_sum, sum_words.get(max_sum)))


def most_messages(df):
    def one_day(name):
        times = df[(df['sender_full_name'] == name)]['timestamp'].dt.strftime('%Y/%m/%d')
        if len(times) == 0:
            return {name: [0, None]}
        else:
            totals = reduce(count_reducer, times, {})
            m_day = max(totals, key=totals.get)
            return {name: [totals.get(m_day), m_day]}

    res = reduce(count_reducer, list(df['sender_full_name']), {})
    most = max(res, key=res.get)
    spam = reduce(flat_reducer, list(map(one_day, names)))
    m_spam = max(spam, key=spam.get)
    print("""
    Chatterbox
    ----------
    Most messages on Zulip: {}, {}
    Most messages on a single day: {}, {} messages on {}
    """.format(most, res.get(most), m_spam, spam.get(m_spam)[0], spam.get(m_spam)[1]))


def most_pictures(df):
    def per_post(name):
        posts = img[(img['sender_full_name'] == name)]['content']
        total = 0
        for post in posts:
            total += len(re.findall('img src', post))
        return {name: total}

    def rate_post(name):
        imgs = len(img[(img['sender_full_name'] == name)])
        all = len(df[(df['sender_full_name'] == name)])
        if all > 0:
            return {name: (imgs / all) * 100}
        else:
            return {name: 0}

    img = df[df['content'].str.contains('img src')]
    all_pics = reduce(flat_reducer, map(per_post, names))
    most = max(all_pics, key=all_pics.get)
    rate = reduce(flat_reducer, map(rate_post, names))
    m_rate = max(rate, key=rate.get)
    if all_pics.get(most) == 0 or rate.get(m_rate) == 0:
        pass
    else:
        print("""
    Worth a Thousand Words
    ----------------------
    Most images posted on Zulip: {}, {}
    Highest % of posts with images: {}, {}%
    """.format(most, all_pics.get(most), m_rate, rate.get(m_rate)))


# this is poorly written. please make it better and submit a pull request!
def most_streams(df):
    def stream_exist(str_name):
        if str_name in df.display_recipient.unique():
            return True
        else:
            return False

    strms = ['help', 'pairing', 'small questions', 'consciousness']
    for each in strms:
        if stream_exist(each):
            if each == 'help':
                help_str = df[(df['display_recipient'] == 'help')]['stream_id'].iloc[0]

                def help_me(name):
                    if not stream_exist('help'):
                        return {None: 'stream does not exist'}
                    help_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == help_str)])
                    return {name: help_post}

                halp = reduce(flat_reducer, map(help_me, names))
                m_help = max(halp, key=halp.get)
                print("""
    MAYDAY! MAYDAY!
    ---------------
    Most messages in 'help' stream:
    {}, {}   
    """.format(m_help, halp.get(m_help)))
            elif each == 'pairing':
                pair_str = df[(df['display_recipient'] == 'pairing')]['stream_id'].iloc[0]

                def be_my_buddy(name):
                    if not stream_exist('help'):
                        return {None: 'stream does not exist'}
                    pair_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == pair_str)])
                    return {name: pair_post}

                buddy = reduce(flat_reducer, map(be_my_buddy, names))
                m_buddy = max(buddy, key=buddy.get)
                print("""
    Buddy System
    ------------
    Most messages in 'pairing' stream: {}, {}
    """.format(m_buddy, buddy.get(m_buddy)))
            elif each == 'small questions':
                q_str = df[(df['display_recipient'] == 'small questions')]['stream_id'].iloc[0]

                def qa(name):
                    q_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == q_str)])
                    return {name: q_post}

                smallq = reduce(flat_reducer, map(qa, names))
                bigq = max(smallq, key=smallq.get)
                print("""
    Biggest Small Question
    ----------------------
    Most messages in 'small questions' stream:
    {}, {}
    """.format(bigq, smallq.get(bigq)))
            elif each == 'consciousness':
                thinks = df[(df['display_recipient'] == 'consciousness')]['stream_id'].iloc[0]

                def thinky(name):
                    con_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == thinks)])
                    return {name: con_post}

                philosopher = reduce(flat_reducer, map(thinky, names))
                galaxy_brain = max(philosopher, key=philosopher.get)
                print("""
    Galaxy Brain
    ------------
    Most messages in 'consciousness' stream:
    {}, {}
    """.format(galaxy_brain, philosopher.get(galaxy_brain)))

    def get_streams(name):
        total = len(df[(df['sender_full_name'] == name)]['stream_id'].unique())
        return {name: total}

    streams = reduce(flat_reducer, map(get_streams, names))
    most = max(streams, key=streams.get)
    print("""
    Jack of All Trades
    ------------------
    Posts in the most unique streams: {}, {}
    """.format(most, streams.get(most)))


def most_tags(df):
    def get_tagger(name):
        posts = df[(df['sender_full_name'] == name)]['content']
        tags = map(lambda x: len(re.findall("user-mention", x)), posts)
        return {name: sum(tags)}

    def get_taggee(name):
        name_only = " ".join([tk.tokenize(name)[0], tk.tokenize(name)[1]])
        tag = "".join(["@", name_only])
        posts = df[(df['content'].str.contains(tag))]
        return {name: len(posts)}

    def get_unique_taggee(name):
        name_only = " ".join([tk.tokenize(name)[0], tk.tokenize(name)[1]])
        tag = "".join(["@", name_only])
        ppl = df[df['content'].str.contains(tag)]['sender_full_name'].unique()
        return {name: len(ppl)}

    taggers = reduce(flat_reducer, map(get_tagger, names))
    m_tagger = max(taggers, key=taggers.get)
    taggees = reduce(flat_reducer, map(get_taggee, names))
    m_taggee = max(taggees, key=taggees.get)
    tag_unique = reduce(flat_reducer, map(get_unique_taggee, names))
    m_tag_unique = max(tag_unique, key=tag_unique.get)
    print("""
    Social Butterflies
    ------------------
    Tagged others most: {}, {} tags
    Tagged most by number: {}, {} tags
    Tagged by most by unique people: {}, {} people
    """.format(m_tagger, taggers.get(m_tagger), m_taggee,
               taggees.get(m_taggee), m_tag_unique, tag_unique.get(m_tag_unique)))


# this is not an award
def clean_data(df):
    # first few times I did this I screwed up Mongo and had thousands of duplicates
    # so let's drop duplicates just to be safe
    df.drop_duplicates('id', inplace=True)
    # drop these extra variables that we don't need
    drop_col = []
    extras = ['_id', 'recipient_id', 'topic_links', 'is_me_message',
              'sender_realm_str', 'type', 'content_type', 'last_edit_timestamp']
    for col in extras:
        if col in df.columns:
            drop_col.append(col)
    df.drop(columns=drop_col, inplace=True)
    # convert timestamps to dates if data came from SQL
    if df.timestamp.dtype == 'int64':
        df.timestamp = df.timestamp.apply(func=lambda x: dt.datetime.fromtimestamp(x))
    # remove messages that are (deleted)
    df.drop(index=df.loc[df['content'] == '<p>(deleted)</p>'].index, inplace=True)
    # remove people from batch who never posted
    drop_absent = []
    global batch_only
    batch_only = df.loc[df['sender_id'].isin(batch_people['user_id'])]
    sums = reduce(count_reducer, batch_only.sender_id, {})
    for k, v in sums.items():
        if v == 0:
            drop_absent.append(k)
    df.drop(index=df[df['sender_id'].isin(drop_absent)].index)
    batch_only = df.loc[df['sender_id'].isin(batch_people['user_id'])]
    return df


if __name__ == "__main__":
    # find members.json and messages db, create zulip_df of messages
    fl = find_files()
    batch_people = pd.read_json(fl.get('members'))
    if fl.get('type') == 'json':
        zulip_df = pd.read_json(fl.get('messages'))
        zulip_df.drop(columns='_id', inplace=True)
    elif fl.get('type') == 'sql':
        import sqlite3 as s3

        zulip_df = pd.read_sql_query("SELECT * FROM messages", s3.connect(fl.get('messages')))
        zulip_df.rename(columns={'timestamp_': 'timestamp'})
    batch_only = pd.DataFrame()
    # clean data, fill batch_only DF
    zulip_df = clean_data(zulip_df)
    streams = zip(zulip_df.display_recipient.unique(), zulip_df.stream_id.unique())
    # # get names of people in batch
    names = batch_only['sender_full_name'].unique()
    # # run awards
    most_links(batch_only)
    most_pictures(batch_only)
    most_messages(batch_only)
    short_long(batch_only)
    most_days(batch_only)
    most_streams(batch_only)
    # receive_emojis(batch_only)
    give_emojis(zulip_df)
    most_tags(zulip_df)
