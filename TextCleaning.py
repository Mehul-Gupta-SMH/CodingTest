import re
from spellchecker import SpellChecker
from functools import lru_cache
from datetime import datetime


@lru_cache
def remove_irrelevant_characters(input_text: str, unwanted_characters_exp = '[:,;,--]' ) -> str:
    """
    Removes unwanted characters from the string
    :param input_text: Text that is to be cleaned
    :param unwanted_characters_exp: Regex for the unwanted characters to be removed (default = '[:,;]')
    :return: cleaned text using regex provided
    """
    # Remove irrelevant characters

    input_text = input_text.strip()
    input_text = re.sub(r"\s{2,}", " ", input_text)
    return str(re.sub(unwanted_characters_exp, '', input_text, flags=re.IGNORECASE))

@lru_cache
def rectify_misspelling(text: str) -> str:
    """
    Corrects mis-spellings in the text provided
    :param text: Text with spelling issues
    :return: Text after removing spelling issues
    """

    # Initialize SpellChecker
    spell = SpellChecker()

    # Split text into words and correct misspellings
    corrected_words = [spell.correction(word) for word in text.split() if spell.correction(word) is not None]

    # Join the corrected words back into text
    try:
        return ' '.join(corrected_words).replace(" i ",".")
    except:
        return ""

@lru_cache
def process_text(text: str, type: str) -> str:
    """
    Processes the string in argument to remove listed special characters and spelling error
    :param text: Unclean text that needs to be processed
    :param type: Type of cleaning activities to be implemented
    :return: cleaned_text
    """
    if not len(text.strip()):
        return text

    cleaned_text = text

    # Caps the size of the entry to 200 characters
    if type == "capping":
        if len(cleaned_text) > 200:
            return cleaned_text.strip()[:199]

    # Removes irrelevant characters from string
    if (type == "all") or ("character_cleaning" in type):
        cleaned_text = remove_irrelevant_characters(cleaned_text)

    if not len(cleaned_text):
        return ""

    # Correct all spelling related issues
    if type == "all" or "misspelling" in type:
        cleaned_text = rectify_misspelling(cleaned_text)

    return cleaned_text



def process_date(text: str, format: str):
    """
    Convert the given text into date time using the format provided
    :param text: Text of the date (to be converted into date time)
    :param format: Format of the date (compatible with datetime package)
    :return: Parsed Datetime from the text
    """
    return datetime.strptime(text.strip(), format)




