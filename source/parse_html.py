from selectolax.parser import HTMLParser
from pathlib import Path
import re
import settings

from source.str_utils import normalize_for_match, normalize_text_to_no_whitespace


def parse_fitch_factiva(html_path):
    """
    Parse a Factiva-exported HTML containing one or multiple Fitch articles.

    Parameters
    ----------
    html_path : str
        Path to the HTML file to parse.

    Returns
    -------
    list of dict
        [
            {
                "title": ...,
                "words": ...,
                "date": ...,
                "category": ...,
                "code": ...,
                "language": ...,
                "copyright": ...,
                "headings": [...],
                "body_text": ...
            },
            ...
        ]
    """
    html_path = Path(html_path)
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    tree = HTMLParser(html_text)

    def clean(text):
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        return re.sub(r"\s+", " ", text).strip()

    def rstrip_colon(text):
        return re.sub(r":\s*$", "", text)

    def is_tail_note(text):
        low = text.lower()
        return ("end of document" in low) or ("©" in text and "factiva" in low)

    # Collect sibling nodes for a single article until the next article header
    def collect_article_nodes(start_div):
        nodes = []
        node = start_div
        while node is not None:
            if node is not start_div and node.css_first("div#hd > span.enHeadline"):
                break
            nodes.append(node)
            node = node.next
        return nodes

    # Collect metadata lines after the header
    def collect_meta_lines(start_div, max_lines=10):
        lines = []
        cur = start_div.next
        for _ in range(max_lines):
            if cur is None or cur.tag != "div":
                break
            text = clean(cur.text())
            if text and len(text) < 250:
                lines.append(text)
            cur = cur.next
        return lines

    # Metadata extraction helpers
    def pick_words(lines):
        for s in lines:
            m = re.search(r"\b(\d{2,6})\s+words\b", s, flags=re.IGNORECASE)
            if m:
                return int(m.group(1))
        return None

    def pick_date(lines):
        pat = r"(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s*\d{4})"
        for s in lines:
            m = re.search(pat, s)
            if m:
                return m.group(1)
        return None

    def pick_category(lines):
        for s in lines:
            if "Fitch" in s and "Commentary" in s:
                return s
        return None

    def pick_code(lines):
        for s in lines:
            if re.fullmatch(r"[A-Z]{4,6}", s):
                return s
        return None

    def pick_language(lines):
        lang_whitelist = {"english", "chinese", "spanish", "portuguese", "french", "german"}
        for s in lines:
            if s.lower() in lang_whitelist:
                return s.title()
        return None

    def pick_copyright(lines):
        for s in lines:
            if "Copyright" in s:
                return s
        return None


    def is_boilerplate(text):
        """
        Pure boilerplate/tail-note detection.
        Return True if this text indicates the end of useful content
        or should be skipped entirely.
        """
        if not text:
            return False
        low = text.lower()
        if "end of document" in low:
            return True
        # Factiva/Dow Jones tail lines or copyright notices
        if "©" in text and ("factiva" in low or "dow jones" in low):
            return True
        # Very short generic anchors to ignore
        if low in {"click here", "read more"}:
            return True
        return False

    # ==== 2) Pure classification ====
    def extract_heading_text(node, text, max_len=150):
        """
        Decide if the current node represents a heading and return the heading text.
        Otherwise return None.

        Parameters
        ----------
        node : selectolax Node
            The current HTML node
        text : str
            The original text (case preserved)
        max_len : int
            Maximum length for heading detection

        Returns
        -------
        str or None
            The detected heading text (with original case preserved),
            or None if not a heading.
        """
        if not text:
            return None

        # ---- Step 1. length filter ----
        first_line = text.splitlines()[0].strip()
        if len(first_line) == 0 or len(first_line) > max_len:
            return None

        # Normalized version for matching
        norm = normalize_for_match(first_line)

        # Keyword search
        for canon, keywords in settings.ALIAS_KEYWORDS.items():
            section_name_without_punctuation = first_line.replace(":", "")
            if norm == canon:
                return section_name_without_punctuation
            for kw in keywords:
                if norm == kw:
                    return section_name_without_punctuation
        return None

    def extract_paragraph_text(node, text):
        """
        Decide if the current node carries a body paragraph and return its text.
        Otherwise return None.
        Rules:
          - prefer <p> nodes; include when class contains 'articleParagraph'
          - as a fallback, accept generic <p> with non-empty text
          - ignore non-<p> by default to avoid noise (tables/lists can be added if needed)
        """
        if not text:
            return None

        if getattr(node, "tag", None) == "p":
            cls = node.attributes.get("class", "") if hasattr(node, "attributes") else ""
            if "articleParagraph" in cls:
                return text
            # fallback: plain <p> with content
            return text

        # If you want to include other tags as paragraphs, enable here:
        # if node.tag in ("div", "span") and len(text) > 0:
        #     return text

        return None

    def _rstrip_colon(s):
        return re.sub(r":\s*$", "", s or "")

    # ==== 3) Orchestration only (no rules here) ====

    def extract_body(article_nodes, meta_lines_count):
        """
        Build nested sections using pure helper functions above.
        Returns:
          sections: [
            {"heading": <str or None>, "paras": [p1, p2, ...], "start_idx": i, "end_idx": j},
            ...
          ]
          materials: dict[str, str]  # { section_name -> concatenated text }
        """
        sections = []
        if not article_nodes:
            return sections, {}

        # Move cursor to the first body node (skip header + meta lines)
        cursor = article_nodes[0]
        for _ in range(meta_lines_count + 1):
            cursor = cursor.next if cursor is not None else None

        cur_heading = None
        cur_paras = []
        linear_nodes = []  # keep count to compute start/end indices

        while cursor is not None:
            # Stop condition: next article header
            if cursor.css_first("div#hd > span.enHeadline"):
                break

            raw = cursor.text()
            txt = normalize_text_to_no_whitespace(raw)

            # Stop on boilerplate/tail-note
            if is_boilerplate(txt):
                break

            # Heading classification (应返回标准化的小写章节名；无则 None)
            htxt = extract_heading_text(cursor, txt)
            if htxt is not None:
                # flush previous block if any
                if cur_paras:
                    sections.append({
                        "heading": cur_heading,
                        "paras": cur_paras,
                        "start_idx": len(linear_nodes) - len(cur_paras),
                        "end_idx": len(linear_nodes)
                    })
                    cur_paras = []
                cur_heading = htxt
            else:
                # Paragraph classification
                ptxt = extract_paragraph_text(cursor, txt)
                if ptxt:
                    cur_paras.append(ptxt)
                    linear_nodes.append(ptxt)

            cursor = cursor.next

        # Flush the last block
        if cur_paras:
            sections.append({
                "heading": cur_heading,
                "paras": cur_paras,
                "start_idx": len(linear_nodes) - len(cur_paras),
                "end_idx": len(linear_nodes)
            })

        # Build materials dict: key = heading (or "_prologue"), value = concatenated text
        materials = {}
        headers = []
        for sec in sections:
            key = sec["heading"] if sec["heading"] else "_prologue"
            text_block = "\n\n".join(sec["paras"]).strip()
            if not text_block:
                continue
            headers.append(key)
            # concat
            if key in materials:
                materials[key] = materials[key].rstrip() + "\n\n" + text_block
            else:
                materials[key] = text_block

        return headers, materials

    # ------------------------
    # Main parsing loop
    # ------------------------
    articles = []
    anchors = tree.css("div#hd > span.enHeadline")

    for headline in anchors:
        start_div = headline.parent
        article_nodes = collect_article_nodes(start_div)
        meta_lines = collect_meta_lines(start_div)

        title = clean(headline.text())
        words = pick_words(meta_lines)
        date = pick_date(meta_lines)
        category = pick_category(meta_lines)
        code = pick_code(meta_lines)
        language = pick_language(meta_lines)
        copyrt = pick_copyright(meta_lines)

        headings, body_text = extract_body(article_nodes, len(meta_lines))

        articles.append({
            "title": title,
            "words": words,
            "date": date,
            "category": category,
            "code": code,
            "language": language,
            "copyright": copyrt,
            "headings": headings,
            "body_text": body_text
        })

    return articles
