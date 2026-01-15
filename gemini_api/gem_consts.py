import json
from google import genai
from google.api_core.exceptions import GoogleAPICallError, ResourceExhausted
import pandas as pd
import asyncio
import csv
import math
from typing import List, Dict, Any
from google.genai import types
import re

GEMINI_MODEL = "gemini-2.0-flash"

BATCH_SIZE = 100
MAX_CONCURRENT_REQUESTS = 50
MAX_RETRIES = 5

PROMPT_BODY = """
    Role: You are an advanced text preprocessing and linguistic analysis assistant.
    Task: 
        You will receive a dictionary of texts (where keys are unique IDs and values are the text strings). 
        Your goal is to process each text separately to prepare it for analysis by LIWC (Linguistic Inquiry and Word Count). 
        You must perform specific cleaning, normalization, and sentiment detection tasks while preserving specific stylistic elements (netspeak).
    
    Input Dictionary: [INSERT_YOUR_TEXT_DICT_HERE]

    Instructions:
    - Language Detection: 
        Identify the primary language of the input text. If none detected, or the language is unclear or unknown, return None.
        Return the language in it's full english name, with the first letter capitalized (e.g., "English", "French", "German", etc).
    - Spelling Correction:
        Correct standard spelling errors (e.g., "becuse" $\rightarrow$ "because").
        Expressive Lengthening: Reduce excessive repeated characters in all words (standard OR slang) to their dictionary form (e.g., "hellooooo" $\rightarrow$ "hello", "looooooppp" $\rightarrow$ "loop", "yesss" $\rightarrow$ "yes", "woooooww" $\rightarrow$ "wow").
        Once normalization is complete, preserve the spelling of internet slang, "netspeak", informal chat language, acronyms, and informal vocabulary. Do not expand or "fix" them into formal English. Do NOT correct internet slang or informal vocabulary EXCEPT to fix expressive lengthening as mentioned. (e.g., keep "lol", "4ever", "brb", "idk" exactly as they appear).
    - Normalization:
        Time Markers: Remove periods and spaces in time formats (e.g., "6 a.m." $\rightarrow$ "6am", "4 P.M." $\rightarrow$ "4pm").
        Acronyms/Locations: Remove periods in standard abbreviations (e.g., "U.S.A." $\rightarrow$ "USA", "e.g." $\rightarrow$ "eg").
        Abbreviation Expansion: Expand standard abbreviations to full words to ensure LIWC dictionary recognition. This includes:
            Calendar: "Jan" $\rightarrow$ "January", "Feb" $\rightarrow$ "February", "Mon" $\rightarrow$ "Monday", etc.
            Titles/Units: "Dr." $\rightarrow$ "Doctor", "lbs" $\rightarrow$ "pounds".
        Identify any string of characters where a lowercase letter is immediately followed by an uppercase letter (e.g., camelCase). Insert a space between them. Do this EXCEPT for the word: "subInstagramname".
        Identify any string where an uppercase letter is followed by another uppercase letter, but then followed by lowercase (handling acronyms like JSONParser -> JSON Parser).
        Do not change the original capitalization of the letters; only insert spaces.
        Do not alter punctuation or words that are already separated.
    - Expansions: 
        Spell out standard contractions or abbreviations where necessary for formal clarity, unless it violates the "netspeak" constraint above.
    - Overall Emoji Sentiment Analysis:
        List all emojis found in the text, but do not remove them from the text.
        Analyze the sentiment of each emoji in the context of the sentence.
        Analyze the combined sentiment of all emojis found.
        Determine the single dominant sentiment (Positive, Negative, or Neutral) based on the most reoccurring or main emotion conveyed by the group of emojis. 
        If no emojis are present, return None.
    - LIWC Optimization:
        Fix Common LIWC Problems: Ensure the text format is optimized for LIWC ingestion.
        Address common formatting issues that hinder LIWC analysis (e.g., Replace "smart quotes" (curled) with straight quotes, remove non-standard symbols that break tokenization, and ensure standard spacing around punctuation).
    - Confidence Assessment:
        Rate your confidence in the accuracy of your corrections and analysis on a scale of 1 to 3.
        Scale Definition: 1 = Highly Confident (The text was clear, corrections were standard). 2 = Moderately Confident (Some ambiguity between typos and netspeak). 3 = Least Confident (The text was very difficult to parse, or the distinction between slang and errors was unclear).
        Provide a brief text explanation justifying this score.

    Output Format:
        Please return the result as a single JSON object containing a list of dictionaries under the key "results". Each dictionary must include the original input ID. All the values of the dictionary must be string.
        JSON{
          "results": [
            {
              "id": "Key_From_Input_Dict",
              "post_language": "English",
              "original_text": "The raw input text...",
              "fixed_text": "The processed text with spelling fixes and normalization...",
              "emoji_sentiment": "Positive",
              "confidence_score": 1,
              "confidence_explanation": "I am rating this a 1 because the spelling errors were standard English mistakes and the netspeak was clearly identifiable.",
              "changes_made_summary": "Brief bullet points of major changes."
            },
            {
              "id": "Next_Key",
              "post_language": "...",
              "original_text": "...",
              "fixed_text": "...",
              "emoji_sentiment": "Negative",
              "confidence_score": 3,
              "confidence_explanation": "High ambiguity in netspeak usage.",
              "changes_made_summary": ""
            }
          ]
        }
"""

# gemini api response example
API_RESULT_EXAMPLE = {
    "results": [
    {
      "id": "CY9uWqiMiAs",
      "post_language": "Unknown",
      "original_text": "2019 !",
      "fixed_text": "2019!",
      "emoji_sentiment": "None",
      "confidence_score": 1,
      "confidence_explanation": "Straightforward case, removing extra space before punctuation.",
      "changes_made_summary": "Removed space before exclamation mark."
    },
    {
      "id": "CYAQYbmARJi",
      "post_language": "German",
      "original_text": "Das 2. lieb ich <33333🦉",
      "fixed_text": "Das 2. lieb ich <33333🦉",
      "emoji_sentiment": "Neutral",
      "confidence_score": 1,
      "confidence_explanation": "German sentence with heart emoticons and an owl emoji. No spelling errors.",
      "changes_made_summary": ""
    }
]}

GEMINI_RESULT_COLUMNS = ["id", "post_language", "original_text", "fixed_text", "emoji_sentiment", "confidence_score",
                         "confidence_explanation", "changes_made_summary", "gemini_status"]
