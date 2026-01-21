import pandas as pd
import os
import time


FINAL_RESULTS_FOLDER = r"final results"
POSTS_RESULTS_FOLDER = r"final results\posts"
COMMS_RESULTS_FOLDER = r"final results\comments"
GEMINI_RESULTS_FOLDER = r"gemini results"
GEMINI_SUCCESS_FOLDER = r"gemini results\success"
GEMINI_FAILS_FOLDER = r"gemini results\fails"
ALL_FOLDER_PATHS = [FINAL_RESULTS_FOLDER, POSTS_RESULTS_FOLDER, COMMS_RESULTS_FOLDER,
                    GEMINI_RESULTS_FOLDER, GEMINI_SUCCESS_FOLDER, GEMINI_FAILS_FOLDER]

STR_NULL_VALUES = ["none", "nan", "nat", "", " "]

POSTS_INFO = {"text_col": "Text", "id_col": "Post ID", "df_type": "posts", "user_id_col": "Username (shortcode)"}
COMMS_INFO = {"text_col": "message", "id_col": "commentId", "post_id_col": "postId", "df_type": "comments", "user_id_col": "username"}
