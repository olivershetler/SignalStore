import re

def to_snake_case(s):

    def preprocess_caps(s):
        """Preprocesses the string to make it easier to convert to snake case.
        Transforms sequnces of caps (acronyms, etc.) so that
        (1) if the sequence is at the end of the string, only the first letter is capitalized,
        (2) if the sequence is in the middle of the string, only the first and last letters are capitalized.
        Also replaces any dashes with underscores, then removes any non-alphanumeric characters remaining (except underscores).
        """
        # Convert any dashes to underscores
        s = s.replace('-', '_')
        # Remove non-alphanumeric characters, except underscores
        s = re.sub(r'[^a-zA-Z0-9_]', '', s)
        # Transform sequences of standard PascalCase / camelCase
        s = re.sub(r'([A-Z])([A-Z]+)(?=[A-Z][a-z])', lambda m: m.group(1) + m.group(2).lower(), s)
        s = re.sub(r'([A-Z]+)(?=[A-Z]|$)', lambda m: m.group(1).capitalize(), s)
        return s

    def to_snake(s):
        """Converts a camel case string to snake case,
        assuming that the string has already been preprocessed."""
        return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

    preprocessed = preprocess_caps(s)

    snake = to_snake(preprocessed)

    return re.sub(r'__+', '_', snake) # Remove any double underscores

def contains_regex_characters(pattern):
    # List of regex special characters that you want to check for
    # You can add more characters to this list if needed
    regex_special_chars = r".^$*+?{}[]\|()"

    # If any special character is in the pattern, it's likely intended as a regex
    return any(char in pattern for char in regex_special_chars)