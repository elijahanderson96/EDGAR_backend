import re
import logging
from Levenshtein import distance as levenshtein_distance
from nltk.corpus import words, wordnet
from nltk.stem import WordNetLemmatizer
import nltk
from database.database import db_connector
nltk.download('words')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

lemmatizer = WordNetLemmatizer()
def get_wordnet_pos(word):
    """Map POS tag to the first character used by WordNetLemmatizer."""
    tag = nltk.pos_tag([word])[0][1][0].upper()  # Get the first character of the POS tag
    tag_dict = {"J": wordnet.ADJ, "N": wordnet.NOUN, "V": wordnet.VERB, "R": wordnet.ADV}
    return tag_dict.get(tag, wordnet.NOUN)  # Default to noun if not found
class ValidateColumns:
    def __init__(self):
        self.valid_words = set(words.words())
        self.commonly_missspelled_words = self.load_unrecognized_words()
        self.corrected_word_list = list(set(word for word in self.commonly_missspelled_words.values() if word))
        self.invalid_words = set()


    def load_unrecognized_words(self):
        """Load unrecognized words and their corrections from the database."""
        query = 'SELECT word, corrected_word FROM metadata.unrecognized_words'
        result = db_connector.run_query(query)

        if not result.empty and 'word' in result.columns and 'corrected_word' in result.columns:
            misspelled_words = result.set_index('word')['corrected_word'].fillna('').to_dict()
        else:
            misspelled_words = {}

        return misspelled_words

    def add_unrecognized_word_to_db(self, word, corrected_word=None):
        """Add a new unrecognized word to the database."""
        corrected_word_value = f"'{corrected_word}'" if corrected_word else 'NULL'
        query = (f"INSERT INTO metadata.unrecognized_words (word, corrected_word) "
                 f"VALUES ('{word}', {corrected_word_value}) ON CONFLICT DO NOTHING")
        db_connector.run_query(query, return_df=False)

    def update_corrected_word_in_db(self, word, corrected_word):
        """Update an existing word's corrected spelling in the database."""
        query = (f"UPDATE metadata.unrecognized_words "
                 f"SET corrected_word = '{corrected_word}' WHERE word = '{word}'")
        db_connector.run_query(query, return_df=False)

    def verify_word_spelling(self, word):
        """Return true if the word exists within a known corpus of valid words, false otherwise."""
        lemmatized_word = lemmatizer.lemmatize(word, get_wordnet_pos(word))
        return lemmatized_word in self.valid_words

    def correct_word_spelling(self, word):
        """Attempt to correct a misspelled word using the dictionary and Levenshtein distance."""
        word_cleaned = re.sub(r'[^\w\s]', '', word).lower()
        correction = self.commonly_missspelled_words.get(word_cleaned)
        if correction:
            return correction
        # If no direct match, find closest word in corrected_word_list within a distance of 1
        min_distance = 1
        closest_word = None

        for correct_word in self.corrected_word_list:
            distance = levenshtein_distance(word_cleaned, correct_word)
            if distance <= min_distance:
                closest_word = correct_word
                min_distance = distance

        if closest_word:
            print(f"Correcting '{word}' to '{closest_word}' with a distance of {min_distance}")
            self.update_corrected_word_in_db(word_cleaned, closest_word)
            return closest_word
        return None

    def validate_line_item(self, line_item):
        """Validate and correct line items based on recognized words."""
        line_item = re.sub(r'-', ' ', line_item)

        if re.search(r'\d', line_item):
            logger.info(f"Disregarded line item (contains number): {line_item}")
            return False, []

        line_item_cleaned = re.sub(r'[^\w\s]', '', line_item.lower())
        words = line_item_cleaned.split()
        unrecognized_words = []

        for word in words:
            if self.verify_word_spelling(word):
                continue
            else:
                corrected_word = self.correct_word_spelling(word)
                if corrected_word:
                    logger.info(f"Corrected '{word}' to '{corrected_word}'.")
                    line_item = line_item.replace(word, corrected_word)
                else:
                    unrecognized_words.append(word)
                    logger.info(f"Unrecognized word: '{word}'")
                    self.add_unrecognized_word_to_db(word)

        if unrecognized_words:
            self.invalid_words.update(unrecognized_words)
            return False, []

        line_item = re.sub(r'\s+', ' ', line_item).replace("_"," ").strip()
        return True, line_item.title()

# Sample usage
if __name__ == "__main__":
    # Assume db_connector is already set up and connected
    validator = ValidateColumns()

    # Sample line items for testing
    sample_line_items = [
        "Accounts recievable, less allowence for credit losses",
        "declarationn benefit",
        "Income Taxes payable, deffered liabilities",
        "Non-existent word and wrong spelled items",
        "632,510  Total stockholders' equity",
        "Increase (decrease) in cash; cash equivalents; and restricted cash"
    ]

    for item in sample_line_items:
        valid, corrected_item = validator.validate_line_item(item)
        print(f"Original: {item}")
        print(f"Valid: {valid}, Corrected: {corrected_item}\n")
