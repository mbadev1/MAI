import sys
import re

class Logger:
    def __init__(self, filepath, terminal=sys.stdout):
        self.terminal = terminal
        self.log = open(filepath, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # Flush both the terminal and the log file to ensure
        # that all messages are up-to-date
        self.terminal.flush()
        self.log.flush()


def has_explicit_repetitions(text: str, min_repeats: int = 3, max_phrase_length: int = 5) -> bool:
    """
    Checks for repetitions of words or phrases in the text, with special attention to the end.

    Args:
        text (str): The input text to check for repetitions.
        min_repeats (int): The minimum number of repetitions to consider excessive.
        max_phrase_length (int): The maximum number of words in a phrase to check for repetitions.

    Returns:
        bool: True if excessive repetitions are found, False otherwise.
    """
    # Normalize whitespace and convert to lowercase for uniformity
    normalized_text = re.sub(r'[^\w\s]', '', text.lower())  # Remove punctuation
    tokens = normalized_text.split()

    # Function to check for repetitions in a given list of tokens
    def check_repetitions(tokens_to_check):
        for phrase_length in range(1, min(max_phrase_length, len(tokens_to_check) // 2) + 1):
            for i in range(len(tokens_to_check) - phrase_length * min_repeats + 1):
                phrase = tokens_to_check[i:i + phrase_length]
                if all(tokens_to_check[i + j*phrase_length:i + (j+1)*phrase_length] == phrase 
                       for j in range(1, min_repeats)):
                    print('REPEATED PHRASE: ', phrase)
                    return phrase
        return None

    # Check the entire text
    repeated_phrase = check_repetitions(tokens)
    if repeated_phrase:
        repetition = True
    else:
        repetition = False

    if repeated_phrase and len(repeated_phrase)==2:
        repetition = False
        
    return repetition