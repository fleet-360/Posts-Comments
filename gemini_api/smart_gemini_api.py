from gemini_api.gem_consts import *
from general.constants import *
from functions.func_consts import *


def build_response_json(response):
    result_text = response.candidates[0].content.parts[0].text
    cut_text = result_text[result_text.find("{"): result_text.rfind("}") + 1]
    result_json = json.loads(cut_text)
    return result_json


# -----------------------------
# GEMINI CALL (SYNC)
# -----------------------------
def call_gemini_sync(prompt: str, gemini_api_key) -> Dict[str, Any]:
    """Synchronous Gemini call using the official client."""
    client = genai.Client(api_key=gemini_api_key)

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )

    results_dict = build_response_json(response)  # convert to python dict
    return results_dict


# -----------------------------
# ASYNC WRAPPER + RETRIES
# -----------------------------
async def call_gemini(prompt: str, gemini_api_key):
    for attempt in range(MAX_RETRIES):
        try:
            return await asyncio.to_thread(call_gemini_sync, prompt, gemini_api_key)

        except (ResourceExhausted, GoogleAPICallError) as e:
            if attempt < MAX_RETRIES - 1:
                print(f"ERROR in call_gemini: {e}. In attempt {attempt}...")
                await asyncio.sleep(2 ** attempt)
            else:
                # raise e
                print("ERROR in call_gemini - reached max retries:", e)
                return None
        except Exception as e:
            print("ERROR in call_gemini:", e)
            return None


# -----------------------------
# PROCESS ONE BATCH
# -----------------------------
async def process_batch(semaphore, texts_dict, gemini_api_key):
    async with semaphore:

        full_prompt = PROMPT_BODY.replace("[INSERT_YOUR_TEXT_DICT_HERE]", str(texts_dict))
        response = await call_gemini(full_prompt, gemini_api_key)

        if response:
            # turn from dict to df
            df_results = pd.DataFrame(response["results"])
            df_results["gemini_status"] = "success"

            return df_results
        else:
            return pd.DataFrame(columns=GEMINI_RESULT_COLUMNS)


# -----------------------------
# WRITE BATCH TO CSV
# -----------------------------
def append_df_to_csv(df: pd.DataFrame, output_file: str, write_header: bool):
    """
    Append a slice of a DataFrame to a CSV file.

    Parameters:
        df_part (pd.DataFrame): The DataFrame slice to write.
        output_file (str): Path to the CSV file.
        write_header (bool): Whether to write the header row.
    """
    if df is None or df.empty:
        return

    df.to_csv(
        output_file,
        mode="a",  # append mode
        header=write_header,  # write header only once
        index=False,  # don't write pandas index
        encoding="utf8"
    )


# -----------------------------
# MAIN PARALLEL PROCESSOR
# -----------------------------
async def process_dataframe(df: pd.DataFrame, text_col: str, id_col: str, gemini_api_key, output_file: str):
    print("in process_dataframe")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    tasks = []
    num_batches = math.ceil(len(df) / BATCH_SIZE)

    headers_df = pd.DataFrame(columns=GEMINI_RESULT_COLUMNS)
    headers_df.to_csv(output_file, mode="w",  header=True, index=False, encoding="utf8")

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_SIZE
        end = start + BATCH_SIZE

        mini_df = df.iloc[start:end]
        if not mini_df.empty:
            mini_dict = mini_df.set_index(id_col)[text_col].to_dict()

            task = asyncio.create_task(process_batch(semaphore, mini_dict, gemini_api_key))
            tasks.append(task)

    with open(output_file, "a", newline="", encoding="utf8") as f:
        for future in asyncio.as_completed(tasks):
            batch_df = await future
            if batch_df is not None and not batch_df.empty:
                batch_df.to_csv(f, header=False, index=False)
    f.close()


def keep_relevant_rows(df, text_col):
    def _is_relevant(text):
        # only keep rows that include either letters or emojis in the text column.
        if str(text) in STR_NULL_VALUES:
            return False
        else:
            relevant_text_pattern = f"[{LANGUAGE_PATTERN}{EMOJIS_RANGE}]"
            found_text = re.findall(relevant_text_pattern, str(text))
            if len(found_text) > 0:
                return True
            else:
                return False

    print("for GEMINI: original len of df:", len(df))
    df["relevant_text"] = df[text_col].apply(_is_relevant)
    filtered_df = df[df["relevant_text"] == True]

    # drop dups by text_col
    filtered_df.drop_duplicates(subset=text_col, keep="first", inplace=True)
    print("for GEMINI: filtered len of df:", len(filtered_df))

    return filtered_df


def merge_gemini_result(df, output_file_path, text_col):
    print("in merge_gemini_result")

    gemini_result = pd.read_csv(output_file_path)
    # merge with gemini results from csv. merge by text instead of id
    df_w_gemini = pd.merge(df, gemini_result, left_on=text_col, right_on="original_text", how="outer")
    # relevant text true and gemini_status none - failed, else none
    df_w_gemini["gemini_status"] = df_w_gemini.apply(lambda row: "failed" if row["relevant_text"] == True and str(row["gemini_status"]) in STR_NULL_VALUES else row["gemini_status"], axis=1)
    return df_w_gemini


def no_gemini_requests(df):
    for col in GEMINI_RESULT_COLUMNS:
        df[col] = None
    return df


# -----------------------------
# ENTRY POINT
# -----------------------------
def process_with_gemini(df, text_col, output_path, id_col, df_type, gemini_api_key):
    print("in process_with_gemini")

    filtered_df = keep_relevant_rows(df, text_col)
    if not filtered_df.empty:
        output_file_path = fr"{output_path}\{GEMINI_RESULTS_FOLDER}\{df_type}_gemini_results.csv"
        asyncio.run(process_dataframe(filtered_df, text_col, id_col, gemini_api_key, output_file_path))
        result_df = merge_gemini_result(df, output_file_path, text_col)
    else:
        # process no gemini - add cols, status - none
        result_df = no_gemini_requests(df)
    return result_df
