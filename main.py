from general.constants import *
from files.open_files import make_dir
from functions.process_functions import process_text_and_emojis, count_posters_comments, clean_df, save_csv_by_language


def main(post_path, comments_path, out_path_folder, gemini_api_key):
    print("in main")
    start_time = time.time()

    make_dir(out_path_folder)

    posts = pd.read_csv(post_path)
    posts = process_text_and_emojis(posts, POSTS_INFO["text_col"], POSTS_INFO["id_col"], POSTS_INFO["df_type"],
                                    POSTS_INFO["user_id_col"], out_path_folder, gemini_api_key)

    comments = pd.read_csv(comments_path)
    comments = process_text_and_emojis(comments, COMMS_INFO["text_col"], COMMS_INFO["id_col"], COMMS_INFO["df_type"],
                                       COMMS_INFO["user_id_col"], out_path_folder, gemini_api_key)

    posts = count_posters_comments(posts, comments, POSTS_INFO["user_id_col"], POSTS_INFO["id_col"],
                                   COMMS_INFO["user_id_col"], COMMS_INFO["post_id_col"])

    posts = clean_df(posts, comms=False)
    comments = clean_df(comments, comms=True)

    save_csv_by_language(posts, POSTS_INFO["df_type"], out_path_folder)
    save_csv_by_language(comments, COMMS_INFO["df_type"], out_path_folder)

    end_time = time.time()
    print(f"FINISHED SCRIPT MAIN: ran for {round((end_time - start_time) / 60, 2)} minutes")
