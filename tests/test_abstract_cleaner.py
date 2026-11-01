import os

os.environ.setdefault("ID", "0")
os.environ.setdefault("MOM_HOST", "localhost")
os.environ.setdefault("INPUT_QUEUE", "norm_in")

from text_processing.abstract_cleaner.abstract_cleaner import clean_abstract  

def test_keeps_repeated_words_in_order_and_lowercases():
    assert clean_abstract("Cat cat CAT dog") == ["cat", "cat", "cat", "dog"]


def test_pure_numbers_dropped_but_alphanumeric_words_kept():
    assert clean_abstract("In 2020 the 1080p and 100th 42") == ["1080p", "100th"]


def test_stopwords_dropped_and_missing_abstract_is_none():
    assert clean_abstract("the and of") == []
    assert clean_abstract(None) is None
