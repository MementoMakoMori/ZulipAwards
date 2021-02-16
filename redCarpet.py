import pandas as pd
from functools import reduce
from itertools import chain
import re
from nltk.tokenize import RegexpTokenizer

# this tokenizer will be used to split words, omitting punctuation
tk = RegexpTokenizer(r'\w+')


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


def most_links(df):

    def remove_img(df):
        with_img = df[df['content'].str.contains("img src")]
        no_img = df.drop(index=with_img.index)
        return no_img

    def get_rate(name):
        if counts.get(name) == 0 or counts.get(name) is None:
            return {name: 0}
        else:
            return {name: counts.get(name)/totals.get(name)}

    df = remove_img(df)
    totals = list(map(lambda x: {x: len(df[(df['sender_full_name'] == x)])}, names))
    totals = reduce(flat_reducer, totals)
    links = df[df['content'].str.contains('<a href')]
    counts = reduce(count_reducer, links['sender_full_name'], {})
    most = max(counts, key=counts.get)
    rates = list(map(get_rate, names))
    rates = reduce(flat_reducer, rates)
    m_rate = max(rates, key=rates.get)
    print("""
    RC Librarians
    ---------------
    Most links in posts: {}, {}
    Most links per post: {}, {}
    (stats for non-image links)
    """.format(most, counts.get(most), m_rate, rates.get(m_rate)))


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

    global checkin_stream
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
        mojis = list(filter(lambda x: x['user']['full_name'] == name, batch_only))
        sum_moji = {}
        for react in mojis:
            k = react['emoji_name']
            sum_moji[k] = sum_moji.get(k, 0) + 1
        return max(sum_moji, key=sum_moji.get)

    only_reacts = list(chain(*filter(lambda x: len(x) > 0, df['reactions'])))
    batch_only = list(filter(lambda x: x['user']['full_name'] in names, only_reacts))
    totals = {}
    for emoji in batch_only:
        totals[emoji['user']['full_name']] = totals.get(emoji['user']['full_name'], 0) + 1
    most = max(totals, key=totals.get)
    fav = fav_emoji(most)
    print("""
    Enthusiastic!
    -------------
    Most reactions to batchmates' posts: {}, {}
    Their favorite emoji: {}
    """.format(most, totals.get(most), fav))


# lmao this one is a mess
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
                if shortest == 0:
                    shortest = 1
        return {name: {"len_l": len_l, "len_s": len_s, "long": longest, "short": shortest}}

    def even_smaller(d):
        short = 1000
        short_name = ""
        for pair in d:
            if len(pair[1]) < short:
                short = len(pair[1])
                short_name = pair[0]
        return short_name

    all = reduce(flat_reducer, map(get_messages, names))
    alldf = pd.DataFrame(data=all.values(), columns=['len_l', 'len_s', 'long', 'short'], index=names)
    m_long = max(alldf['len_l'])
    l_place = alldf[alldf['len_l'] == m_long]
    global m_short
    m_short = min(alldf['len_s'])
    matches = alldf[alldf['len_s'] == m_short]
    if len(matches) > 1:
        toget = list(zip(matches.index, matches['short'].values))
        s_place = alldf.loc[even_smaller(toget)]
    else:
        name = alldf[alldf['len_s'] == m_short].index
        s_place = alldf.loc[name]
    print("""
    Best-Selling Novelist
    ---------------------
    Longest single post by words: {}, {},
     \"{}...\"
    
    Succinct
    --------
    Shortest single post by words: {}, {}, \"{}\"
    (1-word ties broken by n chars)
    """.format(m_long, l_place.index[0], l_place['long'].values[0][:175],
               m_short, s_place.name, s_place['short']))


def long_messages(df):

    def get_messages(name):
        mess = list(df[(df['sender_full_name'] == name)]['content'])
        mess = list(map(remove_code, mess))
        return {name: mess}

    def find_mean_char(texts) -> float:
        lens = list(map(lambda x: len(x), texts))
        return sum(lens)/len(texts)

    def find_mean_word(texts) -> float:
        splits = list(map(lambda x: len(tk.tokenize(x)), texts))
        return sum(splits)/len(texts)

    def sum_all_words(name):
        posts = list(map(lambda x: len(tk.tokenize(x)), name_text.get(name)))
        return {name: sum(posts)}

    global name_text
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
        all = reduce(count_reducer, times, {})
        m_day = max(all, key=all.get)
        return {name: [all.get(m_day), m_day]}

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
            total += len(re.findall("img src", post))
        return {name: total}

    def rate_post(name):
        imgs = len(img[(img['sender_full_name'] == name)])
        all = len(df[(df['sender_full_name'] == name)])
        return {name: (imgs/all)*100}

    img = df[df['content'].str.contains('img src')]
    all_pics = reduce(flat_reducer, map(per_post, names))
    most = max(all_pics, key=all_pics.get)
    rate = reduce(flat_reducer, map(rate_post, names))
    m_rate = max(rate, key=rate.get)
    print("""
    Worth a Thousand Words
    ----------------------
    Most images posted on Zulip: {}, {}
    Highest % of posts with images: {}, {}%
    """.format(most, all_pics.get(most), m_rate, rate.get(m_rate)))


def most_streams(df):
    help_str = df[(df['display_recipient']=='help')]['stream_id'].iloc[0]
    pair_str = df[(df['display_recipient']=='pairing')]['stream_id'].iloc[0]

    def help_me(name):
        help_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == help_str)])
        return {name: help_post}
    def be_my_buddy(name):
        pair_post = len(df[(df['sender_full_name'] == name) & (df['stream_id'] == pair_str)])
        return {name: pair_post}

    def get_streams(name):
        total = len(df[(df['sender_full_name'] == name)]['stream_id'].unique())
        return {name: total}
    streams = reduce(flat_reducer, map(get_streams, names))
    most = max(streams, key=streams.get)
    halp = reduce(flat_reducer, map(help_me, names))
    m_help = max(halp, key=halp.get)
    buddy = reduce(flat_reducer, map(be_my_buddy, names))
    m_buddy = max(buddy, key=buddy.get)
    print("""
    Jack of All Trades
    ------------------
    Posts in the most unique streams: {}, {}

    MAYDAY! MAYDAY!
    ---------------
    Most messages in 'help' stream:
    {}, {}
    
    Buddy System
    ------------
    Most messages in 'pairing' stream: {}, {}
    """.format(most, streams.get(most), m_help, halp.get(m_help), m_buddy, buddy.get(m_buddy)))


def most_tags(df):

    def get_tagger(name):
        posts = df[(df['sender_full_name'] == name)]['content']
        tags = map(lambda x: len(re.findall("user-mention", x)), posts)
        return {name: sum(tags)}

    def get_taggee(name):
        posts = df[df['content'].str.contains("".join(["@", name[:9]]))]
        return {name: len(posts)}

    def get_unique_taggee(name):
        ppl = df[df['content'].str.contains("".join(["@", name[:9]]))]['sender_full_name'].unique()
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
    Tagged most by batchmates: {}, {} tags
    Tagged by most unique batchmates: {}, {} people
    """.format(m_tagger, taggers.get(m_tagger), m_taggee,
               taggees.get(m_taggee), m_tag_unique, tag_unique.get(m_tag_unique)))

# call this if you have extra variables from zulip
def clean_data(df):
    # duplicated posts is a huge issue with Zulip's return! remove them!
    df.drop(index=df[df.id.duplicated()].index, inplace=True)
    # drop these extra variables that we don't need
    drop_col = []
    extras = ['_id', 'recipient_id', 'client', 'topic_links', 'is_me_message',
              'sender_realm_str', 'type', 'content_type', 'last_edit_timestamp']
    for col in extras:
        if col in df.columns:
            drop_col.append(col)
    df.drop(columns=drop_col, inplace=True)
    # remove people who posted <= 5 messages
    drop_absent = pd.array([])
    sums = reduce(count_reducer, df.sender_id, {})
    for id in sums:
        if id <= 5:
            drop_absent.append(id)
    return df.drop(index=df[df['sender_id'] == drop_absent.any()].index)


if __name__ == "__main__":
    zulip_df = pd.read_json("./messages.json")
    zulip_df = clean_data(zulip_df)
    zulip_df = zulip_df[(zulip_df['timestamp'] < '2021-02-13') & (zulip_df['timestamp'] > '2020-10-31')]
    names = zulip_df['sender_full_name'].unique()
    most_links(zulip_df)
    most_pictures(zulip_df)
    most_messages(zulip_df)
    short_long(zulip_df)
    most_days(zulip_df)
    most_streams(zulip_df)
    most_tags(zulip_df)