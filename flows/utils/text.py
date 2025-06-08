def wrap_text(text: str, line_len: int = 120) -> str:
    from textwrap import fill

    def _inner(text):
        for line in text.splitlines():
            yield fill(line, line_len)

    return "\n".join(list(_inner(text)))
