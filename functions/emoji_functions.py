from general.constants import *
from functions.func_consts import *


# for clean text
def remove_emojis(row, text_col):
    # if gemini failed, use original text_col instead.
    text = row["fixed_text"] if str(row["fixed_text"]).lower() not in STR_NULL_VALUES else row[text_col]
    if str(text).lower() not in STR_NULL_VALUES:
        clean_text = re.sub(f"[{EMOJIS_RANGE}]", "", text)
        return clean_text
    else:
        return text


def count_emojis(row, text_col):
    """
    get row with text column. count total emojis, and unique emojis.
    if no text - return (0, 0)
    """

    text = row[text_col]
    if str(text).lower() not in STR_NULL_VALUES:
        # remove skin tones - an emoji with a skin tone is counted as two emojis unless skin tone is removed.
        text_no_skin_tones = re.sub(f"[{EMOJI_SKIN_TONES}]", "", text)
        all_emojis = re.findall(f"[{EMOJIS_RANGE}]", text_no_skin_tones)
        unique_emojis = set(all_emojis)
        counts_tuple = (len(all_emojis), len(unique_emojis))
        return counts_tuple
    else:
        return (0, 0)


def calc_emoji_ratio(row):
    emoji_count = row["emoji_count"]

    text = row["fixed_text_no_emojis"]
    if str(text).lower() not in STR_NULL_VALUES:
        # count words - must include a letter or a number to count as a word
        words_pattern = f"(?=\S*[{LANGUAGE_PATTERN}0-9])\S+"
        words = re.findall(words_pattern, text)
        word_count = len(words)
    else:
        word_count = 0

    if emoji_count == 0:
        return 0
    elif word_count == 0:
        # has emojis but no words in the text
        return 1
    else:
        # has words and emojis
        ratio = round(emoji_count / word_count, 2)
        return ratio


def add_emoji_data(df, text_col):
    print("in add_emoji_data")

    print("in count_emojis")
    df[["emoji_count", "emoji_unique"]] = df.apply(lambda row: count_emojis(row, text_col), axis=1, result_type="expand")
    print("in calc_emoji_ratio")
    df["emoji_ratio"] = df.apply(calc_emoji_ratio, axis=1)
    return df
