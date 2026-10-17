from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from html.parser import HTMLParser

WHITESPACE = re.compile(r"\s+")


class _TextExtractor(HTMLParser):
    """Извлекает видимый текст из HTML, игнорируя script и style."""

    def __init__(self) -> None:
        """Подготавливает парсер и хранилище найденных фрагментов текста."""
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        """Начинает игнорировать содержимое script и style."""
        if tag in {"script", "style"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        """Заканчивает игнорировать содержимое script и style."""
        if tag in {"script", "style"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        """Сохраняет текстовый фрагмент вне script и style."""
        if not self._ignored_depth:
            self.parts.append(data)


def _strip_html(value: str) -> str:
    """Удаляет HTML-разметку и возвращает только видимый текст."""
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return " ".join(parser.parts)


def clean_optional_text(
    value: str | None, *, strip_html: bool = False
) -> str | None:
    """Очищает необязательный текст и заменяет пустой результат на None."""
    if value is None:
        return None
    text = _strip_html(value) if strip_html else html.unescape(value)
    normalized = WHITESPACE.sub(" ", text).strip()
    return normalized or None


def clean_required_text(value: str | None) -> str:
    """Очищает обязательный текст и отклоняет пустое значение."""
    normalized = clean_optional_text(value)
    if normalized is None:
        raise ValueError("required text is blank")
    return normalized


def as_utc(value: datetime) -> datetime:
    """Приводит datetime к UTC, считая naive-значение уже заданным в UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
