from general.constants import *
from functions.func_consts import *


def split_camel_pascal(match):
    before = match.group(1)
    after = match.group(2).lower()
    return before + " " + after


def fix_common_liwc_problems(text, replacements):
    for wrong, correct in replacements.items():
        # Escape the key so special regex chars don't break the pattern
        escaped = re.escape(wrong)

        # Match only when surrounded by spaces or text boundaries - words
        pattern = rf"(?<!\S){escaped}(?!\S)"

        text = re.sub(pattern, correct, text)
    return text


def remove_chars(text):
    try:
        if str(text).lower() not in STR_NULL_VALUES:
            # NFKC converts all fancy-font characters to their standard equivalents
            clean_text = unicodedata.normalize("NFKC", text)

            # fix common liwc problems
            clean_text = fix_common_liwc_problems(clean_text, LIWC_DICT)

            # remove \n
            remove_chars_pattern = "\n|\\n|\\\\n"
            clean_text = re.sub(remove_chars_pattern, " ", clean_text)

            # remove hashtags
            remove_hashtags_pattern = "#[^#\s]+"
            hashtags_list = re.findall(remove_hashtags_pattern, clean_text)
            hashtags_str_list = ", ".join(hashtags_list)
            clean_text = re.sub(remove_hashtags_pattern, " ", clean_text)

            # replace @name with text
            at_replacement = "subInstagramname"
            find_tagged_pattern = "(?<!\S)@\S*(?=$|\s)"
            clean_text = re.sub(find_tagged_pattern, at_replacement, clean_text)

            # remove general chars - keep languages, emojis, numbers, spaces, and punctuation marks.
            keep_chars_pattern = f"[^{LANGUAGE_PATTERN}{EMOJIS_RANGE}{PUNCTUATIONS_PATTERN}{SPACES_PATTERN}\d ]+"
            clean_text = re.sub(keep_chars_pattern, " ", clean_text)

            # replace multiples of same space and punctuation with one occurrence
            replace_multiple_pattern = f"([{PUNCTUATIONS_PATTERN}{SPACES_PATTERN}])\\1+"
            clean_text = re.sub(replace_multiple_pattern, r"\1", clean_text)

            # print("clean text: ", clean_text)
            return (clean_text, hashtags_str_list)
        else:
            return (text, "")
    except Exception as e:
        print("ERROR in remove_chars:", e)
