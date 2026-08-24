#!/usr/bin/env python3
"""Regression tests for upload filename sanitization (GHSA-hhhf-79mm-5w28).

``POST /api/upload`` used to build its destination as ``upload_dir / file.filename``.
The multipart ``filename`` is fully client-controlled, and ``Path``'s ``/`` operator
happily follows ``../`` segments and *replaces* the left side entirely when the right
side is absolute — so an unauthenticated caller could write anywhere the server process
could reach. ``safe_upload_filename()`` reduces the value to a bare basename first.

Pure unit tests: no live backend, no services.
"""

import sys
from pathlib import Path

import pytest

# conftest.py already does this; repeated so the file runs standalone too
sys.path.insert(0, str(Path(__file__).parent.parent / "flexible-graphrag"))

from main import safe_upload_filename  # noqa: E402


# (case_id, hostile filename, expected basename)
TRAVERSAL_CASES = [
    ("posix_relative", "../../../../etc/cron.d/pwn.md", "pwn.md"),
    ("posix_absolute", "/etc/passwd.txt", "passwd.txt"),
    ("windows_relative", r"..\..\Windows\Temp\pwn.md", "pwn.md"),
    ("windows_absolute", r"C:\Windows\System32\pwn.md", "pwn.md"),
    ("windows_drive_relative", "C:pwn.md", "pwn.md"),
    ("unc_path", r"\attacker\share\pwn.md", "pwn.md"),
    ("doubled_dot_slash", "....//....//pwn.md", "pwn.md"),
    ("mixed_separators", "../uploads\..\pwn.md", "pwn.md"),
    ("nested_subdir", "uploads/sub/ok.csv", "ok.csv"),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    "hostile,expected",
    [(c[1], c[2]) for c in TRAVERSAL_CASES],
    ids=[c[0] for c in TRAVERSAL_CASES],
)
def test_directory_components_are_stripped(hostile, expected):
    """Every separator form collapses to a bare basename, on any host OS."""
    assert safe_upload_filename(hostile) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "hostile",
    ["", ".", "..", "./", "../", "\\", "/", "   ", "pwn.md\x00.jpg"],
)
def test_unusable_names_are_rejected(hostile):
    """Names that leave nothing safe behind (or hide a null byte) are refused."""
    assert safe_upload_filename(hostile) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "benign",
    ["report.pdf", "normal file.pdf", "notes-2026.md", "a.b.c.csv", "Ünïcode.txt"],
)
def test_benign_names_pass_through(benign):
    """Ordinary uploads keep their original name (UIs match on saved_as)."""
    assert safe_upload_filename(benign) == benign


@pytest.mark.unit
def test_sanitized_name_stays_inside_upload_dir(tmp_path):
    """The property the handler relies on: join+resolve never escapes upload_dir."""
    upload_root = (tmp_path / "uploads").resolve()
    upload_root.mkdir()

    for _case_id, hostile, _expected in TRAVERSAL_CASES:
        safe_name = safe_upload_filename(hostile)
        assert safe_name is not None
        assert (upload_root / safe_name).resolve().parent == upload_root
