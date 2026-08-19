"""
Web page data source for Flexible GraphRAG using LlamaIndex SimpleWebPageReader.
"""

import os
import uuid
from typing import List, Dict, Any, Optional, Callable
import logging
import requests
from llama_index.core import Document

from .base import BaseDataSource

logger = logging.getLogger(__name__)

# Wikipedia (and many sites) reject requests with no / default Python User-Agent.
_DEFAULT_USER_AGENT = (
    "FlexibleGraphRAG/0.6 (+https://github.com/integratedsemantics/flexible-graphrag; "
    "research/integration-tests)"
)


def _make_web_reader(
    *,
    html_to_text: bool,
    user_agent: str,
    timeout: Optional[int] = 60,
):
    """Build SimpleWebPageReader subclass that sends a User-Agent header.

    Upstream ``load_data`` hardcodes ``headers=None``, which causes Wikipedia
    and similar sites to return a bot-policy notice instead of page content.
    """
    from llama_index.readers.web import SimpleWebPageReader

    class SimpleWebPageReaderWithUA(SimpleWebPageReader):
        def __init__(
            self,
            html_to_text: bool = False,
            metadata_fn: Optional[Callable[[str], Dict]] = None,
            timeout: Optional[int] = 60,
            fail_on_error: bool = False,
            user_agent: str = _DEFAULT_USER_AGENT,
        ) -> None:
            super().__init__(
                html_to_text=html_to_text,
                metadata_fn=metadata_fn,
                timeout=timeout,
                fail_on_error=fail_on_error,
            )
            self._user_agent = user_agent

        def load_data(self, urls: List[str]) -> List[Document]:
            if not isinstance(urls, list):
                raise ValueError("urls must be a list of strings.")
            documents: List[Document] = []
            headers = {"User-Agent": self._user_agent}
            for url in urls:
                try:
                    response = requests.get(
                        url, headers=headers, timeout=self._timeout
                    )
                except Exception:
                    if self._fail_on_error:
                        raise
                    continue

                response_text = response.text
                if response.status_code != 200 and self._fail_on_error:
                    raise ValueError(
                        f"Error fetching page from {url}. server returned status:"
                        f" {response.status_code} and response {response_text}"
                    )

                if self.html_to_text:
                    import html2text
                    response_text = html2text.html2text(response_text)

                metadata: Dict = {"url": url}
                if self._metadata_fn is not None:
                    metadata = self._metadata_fn(url)
                    if "url" not in metadata:
                        metadata["url"] = url

                documents.append(
                    Document(
                        text=response_text,
                        id_=str(uuid.uuid4()),
                        metadata=metadata,
                    )
                )
            return documents

    return SimpleWebPageReaderWithUA(
        html_to_text=html_to_text,
        timeout=timeout,
        user_agent=user_agent,
    )


class WebSource(BaseDataSource):
    """Data source for web pages using LlamaIndex SimpleWebPageReader"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.url = config.get("url", "")
        # Extract readable text instead of raw HTML by default. SimpleWebPageReader defaults to
        # html_to_text=False, which ingests the page's full HTML (inline scripts/CSS/SVG) — a
        # single marketing page can balloon into hundreds of markup "chunks". html_to_text=True
        # runs html2text to keep only the readable content.
        self.html_to_text = bool(config.get("html_to_text", True))
        self.user_agent = (
            config.get("user_agent")
            or os.getenv("WEB_USER_AGENT")
            or _DEFAULT_USER_AGENT
        )

        # Import LlamaIndex web reader
        try:
            self.reader = _make_web_reader(
                html_to_text=self.html_to_text,
                user_agent=self.user_agent,
            )
            logger.info(
                "WebSource initialized for URL: %s (html_to_text=%s)",
                self.url, self.html_to_text,
            )
        except ImportError as e:
            logger.error(f"Failed to import SimpleWebPageReader: {e}")
            raise ImportError("Please install llama-index-readers-web: pip install llama-index-readers-web")
    
    def validate_config(self) -> bool:
        """Validate the web source configuration."""
        if not self.url:
            logger.error("No URL specified for web source")
            return False
        
        if not self.url.startswith(('http://', 'https://')):
            logger.error(f"Invalid URL format: {self.url}")
            return False
        
        return True
    
    def get_documents(self) -> List[Document]:
        """
        Retrieve documents from the web page.
        
        Returns:
            List[Document]: List of LlamaIndex Document objects
        """
        try:
            logger.info(f"Loading web page: {self.url}")
            
            # Use SimpleWebPageReader to load the web page
            documents = self.reader.load_data([self.url])
            
            page_slug = self.url.rstrip("/").split("/")[-1].split("?")[0] or "page"
            file_name = (
                page_slug
                if os.path.splitext(page_slug)[1]
                else f"{page_slug}.txt"
            )
            # Add source metadata
            for doc in documents:
                doc.metadata.update({
                    "source": "web",
                    "url": self.url,
                    "file_path": self.url,
                    "file_name": file_name,
                    "source_type": "web_page",
                })
            
            logger.info(f"WebSource loaded {len(documents)} documents from: {self.url}")
            return documents
            
        except Exception as e:
            logger.error(f"Error loading web page '{self.url}': {str(e)}")
            raise
    
    async def get_documents_with_progress(self, progress_callback=None) -> List[Document]:
        """
        Retrieve documents from the web page with progress tracking.
        
        Args:
            progress_callback: Callback function for progress updates
        
        Returns:
            List[Document]: List of LlamaIndex Document objects
        """
        try:
            logger.info(f"Loading web page: {self.url} with progress tracking")
            
            if progress_callback:
                progress_callback(0, 1, "Connecting to web page...", self.url)
            
            # Use SimpleWebPageReader to load the web page
            documents = self.reader.load_data([self.url])
            
            if progress_callback:
                progress_callback(1, 1, "Processing web page content", self.url)
            
            page_slug = self.url.rstrip("/").split("/")[-1].split("?")[0] or "page"
            file_name = (
                page_slug
                if os.path.splitext(page_slug)[1]
                else f"{page_slug}.txt"
            )
            # Add source metadata
            for doc in documents:
                doc.metadata.update({
                    "source": "web",
                    "url": self.url,
                    "file_path": self.url,
                    "file_name": file_name,
                    "source_type": "web_page",
                })
            
            logger.info(f"WebSource loaded {len(documents)} documents from: {self.url}")
            return (1, documents)  # Return tuple: (1 web page, documents which may be chunks)
            
        except Exception as e:
            logger.error(f"Error loading web page '{self.url}': {str(e)}")
            raise
