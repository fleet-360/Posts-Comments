from general.constants import *
from functions.manual_text_functions import remove_chars
from functions.emoji_functions import add_emoji_data, remove_emojis
from gemini_api.smart_gemini_api import process_with_gemini


def process_text(df, text_col, id_col, output_path, df_type, gemini_api_key):
    print("in process_text")

    print("in remove_chars")
    df[["clean_text_with_emojis", "hashtags"]] = df.apply(lambda row: remove_chars(row[text_col]), axis=1, result_type="expand")
    df["hashtags_count"] = df["hashtags"].apply(lambda h: len(h.split(", ")) if "#" in h else 0)
    df.to_csv(rf"{output_path}\{MID_RESULTS_FOLDER}\{df_type}\{df_type}_before_gemini.csv", index=False)
    df = process_with_gemini(df, "clean_text_with_emojis", output_path, id_col, df_type, gemini_api_key)
    print("in remove_emojis")
    df["fixed_text_no_emojis"] = df.apply(lambda row: remove_emojis(row, text_col), axis=1)
    return df


def complete_user_language(df, user_id_col):
    print("in complete_user_language")

    if any(str(val).lower() in STR_NULL_VALUES for val in df["post_language"].unique()):
        user_language_df = df.groupby(user_id_col)["post_language"].agg(lambda x: x.value_counts().idxmax() if not x.dropna().empty else None).reset_index()
        user_language_dict = user_language_df.set_index(user_id_col)["post_language"].to_dict()
        df["post_language"] = df.apply(lambda row: user_language_dict[row[user_id_col]]
                                                   if (str(row["post_language"]).lower() in STR_NULL_VALUES)
                                                      and (str(row[user_id_col]).lower() not in STR_NULL_VALUES)
                                                   else row["post_language"], axis=1)
    return df


def process_text_and_emojis(df, text_col, id_col, df_type, user_id_col, output_path, gemini_api_key):
    print("in process_text_and_emojis")

    df = process_text(df, text_col, id_col, output_path ,df_type, gemini_api_key)
    df = add_emoji_data(df, text_col)
    df = complete_user_language(df, user_id_col)
    df.to_csv(rf"{output_path}\{MID_RESULTS_FOLDER}\{df_type}\{df_type}_processed_text_and_emojis.csv", index=False)
    return df


def count_posters_comments(posts_df, comms_df, posts_user_col, posts_id_col, comms_user_col, comms_posts_id_col):
    print("in count_posters_comments")

    try:
        mini_posts = posts_df[[posts_user_col, posts_id_col, "Comments Count"]]
        mini_comms = comms_df[[comms_user_col, comms_posts_id_col]]

        merged = pd.merge(mini_posts, mini_comms, left_on=posts_id_col, right_on=comms_posts_id_col, how="inner")
        merged["same user"] = merged.apply(lambda row: True if row[posts_user_col] == row[comms_user_col] else False, axis=1)
        merged = merged[merged["same user"] == True]

        map_self_comments = merged.groupby(posts_id_col)[comms_user_col].size()
        map_df = map_self_comments.to_frame().reset_index()
        map_df.rename(columns={comms_user_col: "engagement_count"}, inplace=True)

        new_posts_df = pd.merge(posts_df, map_df, on=posts_id_col, how="outer")
        new_posts_df["engagement_count"] = new_posts_df["engagement_count"].fillna(0)
        return new_posts_df
    except Exception as e:
        print("ERROR in count_posters_comments:", e)


def clean_df(df, comms):
    print("in clean_df")

    drop_cols = ["clean_text_with_emojis", "relevant_text", "original_text", "id", "id_y", "id_right", "Unnamed: 0"]
    rename_comms_cols = {"id_x": "id", "id_left": "id"}

    df.drop([col for col in df.columns if col in drop_cols], axis=1, inplace=True)
    if comms:
        df.rename(columns=rename_comms_cols, inplace=True)
    return df


def save_csv_by_language(df, df_type, out_folder_path):
    print("in save_csv_by_language")

    # if no language is detected (no letters), it's saved in a separate csv as None.
    df["post_language"] = df["post_language"].astype(str)
    unique_languages_list = list(df["post_language"].unique())
    for language in unique_languages_list:
        mini_df = df[df["post_language"] == language]
        full_out_path = fr"{out_folder_path}\{FINAL_RESULTS_FOLDER}\{df_type}\{df_type}_{str(language)}_csv.csv"
        mini_df.to_csv(full_out_path, index=False)
