from gemini_api.gem_consts import *
from general.constants import *
from functions.func_consts import *


def build_response_json(response):
    try:
        try:
            result_text = response.candidates[0].content.parts[0].text
            fixed_text = repair_json(result_text)
            result_json = json.loads(fixed_text)
            return result_json
        except Exception as regular_convert_error:
            result_text = response.candidates[0].content.parts[0].text
            cut_text = result_text[result_text.find("[") + 1: result_text.rfind("}") + 1]
            text_json = """ {"results": [ """ + cut_text + """ ]} """
            result_json = json.loads(json.dumps(text_json, ensure_ascii=False))
            return result_json
    except Exception as manual_convert_error:
        raise ("ERROR in build_response_json:", manual_convert_error)


async def call_gemini(model, prompt: str):
    # הגדרת הקונפיגורציה (אופציונלי, אפשר להשאיר ריק אם מוגדר ב-Client)
    generation_config = {"response_mime_type": "application/json"}

    for attempt in range(MAX_RETRIES):
        try:
            response = await model.generate_content_async(
                contents=prompt,
                generation_config=generation_config
            )
            print("GEMINI WORKED")
            return build_response_json(response)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                # זיהוי שגיאות עומס
                is_rate_limit = "429" in str(e) or "ResourceExhausted" in str(e)
                msg = "Rate Limit" if is_rate_limit else "Error"

                print(f"{msg} in call_gemini: {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                print(f"FAILED all {MAX_RETRIES} call_gemini attempts: {e}")

    return None


# -----------------------------
# PROCESS ONE BATCH
# -----------------------------
async def process_batch(semaphore, texts_dict, model):
    async with semaphore:

        full_prompt = PROMPT_BODY.replace("[INSERT_YOUR_TEXT_DICT_HERE]", str(texts_dict))
        response = await call_gemini(model, full_prompt)

        try:
            if response and isinstance(response, dict) and "results" in response.keys():
                # turn from dict to df
                df_results = pd.DataFrame(response["results"])
                df_results["gemini_status"] = "success"

                return df_results
            else:
                return pd.DataFrame(columns=GEMINI_RESULT_COLUMNS)
        except Exception as e:
            return pd.DataFrame(columns=GEMINI_RESULT_COLUMNS)


# -----------------------------
# MAIN PARALLEL PROCESSOR
# -----------------------------
async def process_dataframe(df: pd.DataFrame, text_col: str, id_col: str, gemini_api_key, output_file: str):
    print("in process_dataframe")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel(GEMINI_MODEL)

    tasks = []
    num_batches = math.ceil(len(df) / BATCH_SIZE)

    if not os.path.exists(output_file):
        headers_df = pd.DataFrame(columns=GEMINI_RESULT_COLUMNS)
        headers_df.to_csv(output_file, mode="w",  header=True, index=False, encoding="utf8")

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_SIZE
        end = start + BATCH_SIZE

        mini_df = df.iloc[start:end]
        if not mini_df.empty:
            mini_dict = mini_df.set_index(id_col)[text_col].to_dict()
            dict_json_str = json.dumps(mini_dict, ensure_ascii=False)

            task = asyncio.create_task(process_batch(semaphore, dict_json_str, model))
            tasks.append(task)

    with open(output_file, "a", newline="", encoding="utf8") as f:
        for future in asyncio.as_completed(tasks):
            batch_df = await future
            if batch_df is not None and not batch_df.empty:
                batch_df.to_csv(f, header=False, index=False)
    f.close()


def is_relevant(text):
    # only keep rows that include either letters or emojis in the text column.
    if str(text).lower() in STR_NULL_VALUES:
        return False
    else:
        relevant_text_pattern = f"[{LANGUAGE_PATTERN}{EMOJIS_RANGE}]"
        found_text = re.findall(relevant_text_pattern, str(text))
        if len(found_text) > 0:
            return True
        else:
            return False


def keep_relevant_rows(df, text_col, output_path):
    print("in keep_relevant_rows")

    print("for GEMINI: original len of df:", len(df))

    if os.path.exists(output_path):
        # if past results exist, remove rows that already have a past result, by text_col.
        existing_results = pd.read_csv(output_path,names=GEMINI_RESULT_COLUMNS, usecols=range(len(GEMINI_RESULT_COLUMNS)))  #
        df = df[~df[text_col].isin(existing_results["original_text"])]

    filtered_df = df[df["relevant_text"] == True]

    # drop dups by text_col
    filtered_df.drop_duplicates(subset=text_col, keep="first", inplace=True)
    print("for GEMINI: filtered len of df:", len(filtered_df))

    return filtered_df


def merge_gemini_result(df, output_file_path, text_col):
    print("in merge_gemini_result")

    # gemini_result = pd.read_csv(output_file_path, names=GEMINI_RESULT_COLUMNS, usecols=range(len(GEMINI_RESULT_COLUMNS)))
    # # merge with gemini results from csv. merge by text instead of id
    # df_w_gemini = pd.merge(df, gemini_result, left_on=text_col, right_on="original_text", how="outer")

    gemini_df = pd.read_csv(output_file_path, names=GEMINI_RESULT_COLUMNS, usecols=range(len(GEMINI_RESULT_COLUMNS)))
    gemini_df = gemini_df.astype("string")  # pandas' nullable string dtype
    gemini_pl = pl.from_dataframe(gemini_df)

    df = df.astype("string")  # pandas' nullable string dtype
    df_pl = pl.from_dataframe(df)

    merged_pl = df_pl.join(gemini_pl, left_on=text_col, right_on="original_text", how="left")
    df_w_gemini = merged_pl.to_pandas()

    # relevant text true and gemini_status none - failed, else none
    df_w_gemini["gemini_status"] = df_w_gemini.apply(lambda row: "failed" if
                                                                 row["relevant_text"] == True and
                                                                 str(row["gemini_status"]).lower() in STR_NULL_VALUES
                                                                 else row["gemini_status"], axis=1)
    return df_w_gemini


def save_gemini_fails(df, output_path, df_type):
    print("in save_gemini_fails")

    failed_df = df[df["gemini_status"] == "failed"]
    print(f"amount of gemini fails for df {df_type}: {len(failed_df)}")
    failed_file_path = fr"{output_path}\{GEMINI_FAILS_FOLDER}\{df_type}_gemini_fails.csv"
    failed_df.to_csv(failed_file_path, index=False)


def no_gemini_requests(df):
    for col in GEMINI_RESULT_COLUMNS:
        df[col] = None
    return df


# -----------------------------
# ENTRY POINT
# -----------------------------
def process_with_gemini(df, text_col, output_path, id_col, df_type, gemini_api_key):
    print("in process_with_gemini")

    df["relevant_text"] = df[text_col].apply(is_relevant)

    output_file_path = fr"{output_path}\{GEMINI_SUCCESS_FOLDER}\{df_type}_gemini_results.csv"
    filtered_df = keep_relevant_rows(df, text_col, output_file_path)
    if not filtered_df.empty:
        asyncio.run(process_dataframe(filtered_df, text_col, id_col, gemini_api_key, output_file_path))
        result_df = merge_gemini_result(df, output_file_path, text_col)
        result_df.to_csv(rf"{output_path}\{MID_RESULTS_FOLDER}\{df_type}\{df_type}_merged_with_gemini.csv", index=False)
        save_gemini_fails(result_df, output_path, df_type)
    else:
        # process no gemini - add cols, status - none
        result_df = no_gemini_requests(df)
    return result_df
