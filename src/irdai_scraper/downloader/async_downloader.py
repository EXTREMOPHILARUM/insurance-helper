"""Async file downloader with parallel download support."""

import asyncio
from pathlib import Path
from typing import Callable, Optional

import aiofiles
import aiohttp
from aiolimiter import AsyncLimiter
from rich.console import Console

from ..config import ScraperConfig
from ..models import DownloadResult, DownloadTask

console = Console()


class AsyncFileDownloader:
    """High-performance async file downloader."""

    def __init__(
        self,
        config: ScraperConfig,
        max_concurrent: int = 10,
        rate_limit: float = 10.0,  # requests per second
    ):
        self.config = config
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = AsyncLimiter(rate_limit, 1)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            ssl=False,  # IRDAI has SSL issues
        )
        timeout = aiohttp.ClientTimeout(total=self.config.download_timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": self.config.user_agent},
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def download_file(
        self,
        url: str,
        destination: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> DownloadResult:
        """Download a single file with retry logic."""
        async with self.semaphore:
            await self.rate_limiter.acquire()

            for attempt in range(self.config.retry_attempts):
                try:
                    return await self._do_download(url, destination, progress_callback)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt == self.config.retry_attempts - 1:
                        return DownloadResult(url=url, success=False, error=str(e))
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                except Exception as e:
                    return DownloadResult(url=url, success=False, error=str(e))

            return DownloadResult(url=url, success=False, error="Max retries exceeded")

    async def _do_download(
        self,
        url: str,
        destination: Path,
        progress_callback: Optional[Callable[[int, int], None]],
    ) -> DownloadResult:
        """Perform the actual download."""
        # Ensure parent directory exists
        destination.parent.mkdir(parents=True, exist_ok=True)

        async with self.session.get(url, allow_redirects=True) as response:
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            async with aiofiles.open(destination, "wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        progress_callback(downloaded, total_size)

            return DownloadResult(
                url=url,
                success=True,
                file_path=destination,
                file_size=downloaded,
            )

    async def download_task(self, task: DownloadTask) -> DownloadResult:
        """Download a single task."""
        return await self.download_file(task.url, task.destination)

    async def download_batch(
        self,
        tasks: list[DownloadTask],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> list[DownloadResult]:
        """Download multiple files concurrently.

        Args:
            tasks: List of download tasks
            progress_callback: Optional callback(completed, total, current_url)
        """
        results = []
        total = len(tasks)

        async def download_with_progress(task: DownloadTask, index: int) -> DownloadResult:
            result = await self.download_task(task)
            if progress_callback:
                progress_callback(index + 1, total, task.url)
            return result

        # Create tasks for all downloads
        download_tasks = [
            download_with_progress(task, i) for i, task in enumerate(tasks)
        ]

        # Run all downloads concurrently (semaphore limits actual concurrency)
        results = await asyncio.gather(*download_tasks, return_exceptions=True)

        # Convert exceptions to DownloadResult
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(
                    DownloadResult(
                        url=tasks[i].url,
                        success=False,
                        error=str(result),
                    )
                )
            else:
                processed_results.append(result)

        return processed_results
