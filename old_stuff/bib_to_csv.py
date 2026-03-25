#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

import pandas as pd
import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import convert_to_unicode
from pylatexenc.latex2text import LatexNodes2Text


def latex_to_text(s: str) -> str:
    """Convert LaTeX markup to plain text (accents, commands)."""
    if not s:
        return ""
    return LatexNodes2Text().latex_to_text(s).strip()


def normalize_authors(author_field: str) -> str:
    """
    BibTeX authors are 'First Last and Second Last'.
    Convert to 'Last, First; Last, First' for CSV.
    """
    if not author_field:
        return ""
    parts = [p.strip() for p in author_field.split(" and ") if p.strip()]
    cleaned = []
    for p in parts:
        # Remove LaTeX first
        p = latex_to_text(p)
        # Split "Last, First" vs "First Last"
        if "," in p:
            last, first = [x.strip() for x in p.split(",", 1)]
        else:
            toks = p.split()
            if len(toks) == 1:
                last, first = toks[0], ""
            else:
                # Heuristic: last token is last name
                last, first = toks[-1], " ".join(toks[:-1])
        cleaned.append(f"{last}, {first}".strip().rstrip(","))
    return "; ".join(cleaned)


def normalize_keywords(kw_field: str) -> str:
    """Turn comma/semicolon separated keywords into '; ' separated list."""
    if not kw_field:
        return ""
    s = latex_to_text(kw_field)
    # Split on commas/semicolons
    parts = re.split(r"[;,]\s*", s)
    parts = [p.strip() for p in parts if p.strip()]
    return "; ".join(parts)


def get_field(entry: dict, name: str) -> str:
    """Get a field safely and clean LaTeX."""
    val = entry.get(name, "") or ""
    return latex_to_text(val)


def parse_bib(bib_path: Path) -> pd.DataFrame:
    parser = BibTexParser(common_strings=True)
    parser.customization = convert_to_unicode  # ensure unicode
    with bib_path.open("r", encoding="utf-8") as fh:
        db = bibtexparser.load(fh, parser=parser)

    rows = []
    for e in db.entries:
        # Citation key is stored under 'ID' in bibtexparser
        citekey = e.get("ID", "")
        entrytype = e.get("ENTRYTYPE", "")

        # Common fields across item types
        title = get_field(e, "title")
        authors = normalize_authors(e.get("author", ""))
        year = get_field(e, "year")
        date = get_field(e, "date")  # sometimes BBT populates 'date'
        journal = get_field(e, "journal")
        booktitle = get_field(e, "booktitle")
        publisher = get_field(e, "publisher")
        volume = get_field(e, "volume")
        number = get_field(e, "number")
        pages = get_field(e, "pages")
        doi = get_field(e, "doi")
        url = get_field(e, "url")
        issn = get_field(e, "issn")
        isbn = get_field(e, "isbn")
        abstract = get_field(e, "abstract")
        keywords = normalize_keywords(e.get("keywords", ""))

        # Example of extracting additional BBT specifics if present
        # (BBT sometimes stores extra in 'note' or custom fields)
        note = get_field(e, "note")

        rows.append({
            "citationKey": citekey,
            "type": entrytype,
            "title": title,
            "authors": authors,
            "year": year or date,
            "journal": journal,
            "booktitle": booktitle,
            "publisher": publisher,
            "volume": volume,
            "number": number,
            "pages": pages,
            "doi": doi,
            "url": url,
            "issn": issn,
            "isbn": isbn,
            "keywords": keywords,
            "abstract": abstract,
            "note": note,
        })

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(
        description="Convert BetterBibTeX .bib to CSV including citation keys."
    )
    ap.add_argument("bibfile", type=Path, help="Input .bib path")
    ap.add_argument(
        "-o", "--output", type=Path, default=None, help="Output .csv path (default: same stem)"
    )
    args = ap.parse_args()

    df = parse_bib(args.bibfile)

    out = args.output or args.bibfile.with_suffix(".csv")
    # Write CSV with UTF-8 and safe newline handling
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
