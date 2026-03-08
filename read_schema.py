import re

from schema import schema


def test_regex(filename: str):
    for name, value in schema.items():
        pattern = value["pattern"]
        if search := re.search(pattern, filename, re.IGNORECASE):
            return {
                "name": name,
                "groups": search.groupdict(),
                "columns": value.get("columns"),
                "pattern": value.get("pattern")
            }

    raise LookupError(filename)
