import pandas as pd
import re
import unicodedata


LANGUAGE_PATTERN = "A-Za-zÀ-ÖØ-öø-ÿĀ-žƀ-ɏЀ-ӿΑ-ωΆ-ώԱ-Ֆա-ֆა-ჰ"  # all European languages
# # include fonts
# FANCY_LANGUAGE_PATTERN = "A-Za-zÀ-ÖØ-öø-ÿĀ-žƀ-ɏЀ-ӿΑ-ωΆ-ώԱ-Ֆա-ֆა-ჰ\u1D00-\u1D7F\u1D80-\u1DBF\u1E00-\u1EFF\u2070-\u209F\u2100-\u214F\u2150-\u218F\u2460-\u24FF\u2C00-\u2C5F\u2C60-\u2C7F\u2DE0-\u2DFF\uA640-\uA69F\uA720-\uA7FF\uFF00-\uFFEF\U0001D400-\U0001D7FF"

PUNCTUATIONS_PATTERN = "!-\/:-@\[-`{-~\u2000–\u206F\u3000-\u303F"

SPACES_PATTERN = "\u0020\u00A0\u2000-\u200A\u3000"

# modern emoji range: \u1F300-\u1FAFF
EMOJIS_RANGE = (
    "\U0001F600-\U0001F64F"  # Emoticons
    "\U0001F300-\U0001F5FF"  # Symbols & pictographs
    "\U0001F680-\U0001F6FF"  # Transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # Flags
    "\U00002700-\U000027BF"  # Dingbats
    "\U0001F900-\U0001F9FF"  # Supplemental symbols
    "\U0001FA70-\U0001FAFF"  # Extended-A
    "\U00002600-\U000026FF"  # Misc symbols
    "\U0001F700-\U0001F77F"  # Alchemical symbols
)
EMOJI_SKIN_TONES = "\U0001F3FB-\U0001F3FF"

# dict of common LIWC problems guidelines
LIWC_DICT = {
    "w/": "with",
    "b/": "between",
    "&": "and",
    "'cause": "because",
    "and/or": "and - or",
    "'an": "and",
    "'n": "and",
    "mos": "months",
    "sec": "second"
}
