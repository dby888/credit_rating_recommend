import re


def normalize_for_match(s: str) -> str:
    """Lowercase + remove punctuation and extra spaces for matching only."""
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)  # 非字母数字都替换为空格
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_text_to_no_whitespace(s):
    """
    Pure text normalization:
    - replace non-breaking space
    - collapse repeated whitespace
    - strip leading/trailing spaces
    """
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()
