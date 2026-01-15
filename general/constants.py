import pandas as pd
import os
import time


FINAL_RESULTS_FOLDER = r"final results"
POSTS_RESULTS_FOLDER = r"final results\posts"
COMMS_RESULTS_FOLDER = r"final results\comments"
GEMINI_RESULTS_FOLDER = r"gemini results"

STR_NULL_VALUES = ["none", "nan", "nat", "", " "]

POSTS_INFO = {"text_col": "Text", "id_col": "Post ID", "df_type": "posts", "user_id_col": "Username (shortcode)"}
COMMS_INFO = {"text_col": "message", "id_col": "commentId", "post_id_col": "postId", "df_type": "comments", "user_id_col": "username"}
