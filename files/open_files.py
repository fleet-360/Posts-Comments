from general.constants import *


def make_dir(outpath_folder):
    print("in make_dir")

    os.makedirs(outpath_folder, exist_ok=True)
    for folder_path in ALL_FOLDER_PATHS:
        os.makedirs(fr"{outpath_folder}\{folder_path}", exist_ok=True)


# doesn't fully work - csv too big, tsv bad data/separator.
def table_to_multi_sheet_excel(input_path, excel_path, rows_per_sheet=1_000_000):
    """
    Convert a large CSV/TSV file into an Excel workbook with multiple sheets.

    Parameters:
        input_path (str): Path to the input CSV/TSV file.
        excel_path (str): Path to the output XLSX file.
        rows_per_sheet (int): Max rows per sheet (Excel limit is 1,048,576).
    """

    # Detect file extension
    ext = os.path.splitext(input_path)[1].lower()
    print("ext", ext)

    # Choose separator based on extension
    if ext == ".tsv":
        sep = "\t"
    elif ext == ".csv":
        sep = ","
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    # Create Excel writer
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:

        # Read in chunks
        chunk_iter = pd.read_csv(input_path, sep=sep, engine="python", chunksize=rows_per_sheet)

        for i, chunk in enumerate(chunk_iter, start=1):
            sheet_name = f"Sheet_{i}"
            chunk.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Done! Data written to {excel_path}")


def tsv_to_csv(in_path, out_path):
    # fixes error: pandas.errors.ParserError: '	' expected after '"'
    df = pd.read_csv(
        in_path,
        sep="\t",
        engine="python",
        quoting=3,          # 3 = csv.QUOTE_NONE
        escapechar="\\",    # allow escaping if present
        on_bad_lines="warn"
    )
    df.to_csv(out_path)
