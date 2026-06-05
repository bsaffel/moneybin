"""Shared column-name classification regexes for PDF transaction tables.

Two modules use these patterns:

* ``auto_derive`` matches them against original PDF table column headers
  ("Transaction Amount", "Withdrawals", "Deposits") to infer the sign
  convention and identify the amount columns.
* ``routing`` matches them against the recipe ``FieldExtraction.name``
  (which preserves the original column header) to canonicalise row-dict
  keys ("Withdrawals" → ``"debit"``, "Deposit Amount" → ``"credit"``).

Centralising the regexes keeps the two call sites coherent — adding a
synonym ("withdrawal" alongside "withdraw") updates both column
classification AND row canonicalisation in one place.
"""

from __future__ import annotations

import re

DEBIT_NAME_RE = re.compile(r"debit|withdraw", re.IGNORECASE)
CREDIT_NAME_RE = re.compile(r"credit|deposit", re.IGNORECASE)
AMOUNT_NAME_RE = re.compile(r"amount", re.IGNORECASE)
# Header pattern that matches the transaction-date column header
# (``^(date|trans.*date|posting.*date)$``). Used by auto_derive to find
# the date column on the original table header. routing uses a separate
# ``POST_DATE_NAME_RE`` to distinguish "Posting Date" from "Transaction
# Date" at canonical-key time.
DATE_HEADER_RE = re.compile(r"^(date|trans.*date|posting.*date)$", re.IGNORECASE)
POST_DATE_NAME_RE = re.compile(r"post(?:ing)?\s*date", re.IGNORECASE)
DESC_NAME_RE = re.compile(r"description|memo|payee", re.IGNORECASE)
