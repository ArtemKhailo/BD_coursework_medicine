"""Доменна служба: нормалізація пошукового запиту."""
from __future__ import annotations


class QueryNormalizer:
    def normalize(self, raw: str) -> str:
        """Нижній регістр + слова за алфавітом -> стабільний ключ кешу.

        «Bacterial Infections» і «infections bacterial» мають давати однаковий результат.
        Та сама логіка, що й normalize_query у scripts/process_dataset.py.
        """
        return " ".join(sorted((raw or "").lower().split()))
