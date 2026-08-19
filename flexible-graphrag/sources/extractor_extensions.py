"""
Shared file-extension set used by PassthroughExtractor and BytesCaptureExtractor.

All sources (S3, GCS, Azure Blob, Filesystem) register these extensions with
their LlamaIndex reader so that every recognised file type goes through the
extractor rather than the reader's built-in parser.

Keep this list in one place so adding a new format only requires one edit.
"""

PASSTHROUGH_EXTENSIONS: frozenset = frozenset({
    # Documents
    ".pdf",
    ".docx", ".doc",
    ".pptx", ".ppt",
    ".xlsx", ".xls",
    ".odt", ".ods", ".odp",
    # Text / markup
    ".txt", ".md", ".rst",
    ".html", ".htm",
    ".xml",
    ".csv", ".tsv",
    ".json", ".jsonl",
    # Images (for OCR via Docling)
    ".png", ".jpg", ".jpeg", ".gif",
    ".tif", ".tiff", ".bmp", ".webp",
    ".svg",
    # Other
    ".rtf",
    ".eml", ".msg",
})
