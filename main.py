from general.constants import *
from files.open_files import make_dir, keep_relevant_profiles
from functions.process_functions import process_text_and_emojis, count_posters_comments, clean_df, save_csv_by_language


def main(profiles_path, posts_path, comments_path, out_path_folder, gemini_api_key):
    print("in main")
    start_time = time.time()

    make_dir(out_path_folder)

    profiles = pd.read_csv(profiles_path)

    posts = keep_relevant_profiles(profiles, posts_path, PROFILES_INFO["user_id_col"], POSTS_INFO["user_id_col"])
    posts = process_text_and_emojis(posts, POSTS_INFO["text_col"], POSTS_INFO["id_col"], POSTS_INFO["df_type"],
                                    POSTS_INFO["user_id_col"], out_path_folder, gemini_api_key)

    comments = keep_relevant_profiles(profiles, comments_path, PROFILES_INFO["user_id_col"], COMMS_INFO["user_id_col"])
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


if __name__ == '__main__':
    from private.private_consts import *
    main(PROFILES_ORIGINAL_PATH, POSTS_ORIGINAL_PATH, COMMS_CSV_PATH,
         OUT_PATH_FOLDER, GEMINI_API_KEY)
