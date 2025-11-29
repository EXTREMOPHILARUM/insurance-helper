"""JSON-based state management for resume capability."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..config import ProductType, ScraperConfig
from ..models import FailedDownload, ScraperState, SessionState


class StateManager:
    """Manages scraper state using JSON file for resume capability."""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.state_file = config.data_dir / "state.json"
        self._state: Optional[ScraperState] = None

    def _load_state(self) -> ScraperState:
        """Load state from JSON file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)

                # Convert sessions dict
                sessions = {}
                for key, val in data.get("sessions", {}).items():
                    sessions[key] = SessionState(**val)

                # Convert failed downloads
                failed = [FailedDownload(**fd) for fd in data.get("failed_downloads", [])]

                return ScraperState(
                    sessions=sessions,
                    completed_downloads=set(data.get("completed_downloads", [])),
                    failed_downloads=failed,
                    last_updated=datetime.fromisoformat(data["last_updated"])
                    if "last_updated" in data
                    else datetime.utcnow(),
                )
            except (json.JSONDecodeError, KeyError, ValueError):
                # Corrupted state file, start fresh
                pass

        return ScraperState()

    def _save_state(self) -> None:
        """Save state to JSON file."""
        if self._state is None:
            return

        # Ensure directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert to JSON-serializable format
        data = {
            "sessions": {
                key: {
                    "last_completed_page": val.last_completed_page,
                    "status": val.status,
                    "total_products": val.total_products,
                    "started_at": val.started_at.isoformat() if val.started_at else None,
                    "completed_at": val.completed_at.isoformat() if val.completed_at else None,
                }
                for key, val in self._state.sessions.items()
            },
            "completed_downloads": list(self._state.completed_downloads),
            "failed_downloads": [
                {
                    "url": fd.url,
                    "error": fd.error,
                    "retries": fd.retries,
                    "last_attempt": fd.last_attempt.isoformat(),
                }
                for fd in self._state.failed_downloads
            ],
            "last_updated": datetime.utcnow().isoformat(),
        }

        with open(self.state_file, "w") as f:
            json.dump(data, f, indent=2)

    @property
    def state(self) -> ScraperState:
        """Get current state, loading from file if needed."""
        if self._state is None:
            self._state = self._load_state()
        return self._state

    def get_session(self, product_type: ProductType) -> SessionState:
        """Get or create session state for a product type."""
        key = product_type.value
        if key not in self.state.sessions:
            self.state.sessions[key] = SessionState()
        return self.state.sessions[key]

    def start_session(self, product_type: ProductType) -> SessionState:
        """Start or resume a scraping session."""
        session = self.get_session(product_type)
        if session.status != "running":
            session.status = "running"
            session.started_at = datetime.utcnow()
        self._save_state()
        return session

    def update_page_progress(self, product_type: ProductType, page: int) -> None:
        """Update the last completed page for a session."""
        session = self.get_session(product_type)
        session.last_completed_page = page
        self.state.last_updated = datetime.utcnow()
        self._save_state()

    def get_last_completed_page(self, product_type: ProductType) -> int:
        """Get the last completed page for a session."""
        return self.get_session(product_type).last_completed_page

    def complete_session(self, product_type: ProductType, total_products: int) -> None:
        """Mark a session as completed."""
        session = self.get_session(product_type)
        session.status = "completed"
        session.completed_at = datetime.utcnow()
        session.total_products = total_products
        self._save_state()

    def fail_session(self, product_type: ProductType, error: str) -> None:
        """Mark a session as failed."""
        session = self.get_session(product_type)
        session.status = "failed"
        self._save_state()

    def is_download_completed(self, url: str) -> bool:
        """Check if a URL has already been downloaded."""
        return url in self.state.completed_downloads

    def mark_download_completed(self, url: str) -> None:
        """Mark a URL as downloaded."""
        self.state.completed_downloads.add(url)
        self._save_state()

    def mark_download_failed(self, url: str, error: str) -> None:
        """Record a failed download."""
        # Update existing or add new
        for fd in self.state.failed_downloads:
            if fd.url == url:
                fd.error = error
                fd.retries += 1
                fd.last_attempt = datetime.utcnow()
                self._save_state()
                return

        self.state.failed_downloads.append(
            FailedDownload(url=url, error=error, retries=1)
        )
        self._save_state()

    def get_failed_downloads(self) -> list[FailedDownload]:
        """Get all failed downloads."""
        return self.state.failed_downloads

    def clear_failed_download(self, url: str) -> None:
        """Remove a URL from failed downloads (after successful retry)."""
        self.state.failed_downloads = [
            fd for fd in self.state.failed_downloads if fd.url != url
        ]
        self._save_state()

    def reset_session(self, product_type: ProductType) -> None:
        """Reset a session to start from scratch."""
        key = product_type.value
        if key in self.state.sessions:
            self.state.sessions[key] = SessionState()
            self._save_state()

    def reset_all(self) -> None:
        """Reset all state."""
        self._state = ScraperState()
        self._save_state()

    def get_summary(self) -> dict:
        """Get a summary of current state."""
        return {
            "sessions": {
                key: {
                    "status": val.status,
                    "last_page": val.last_completed_page,
                    "total_products": val.total_products,
                }
                for key, val in self.state.sessions.items()
            },
            "completed_downloads": len(self.state.completed_downloads),
            "failed_downloads": len(self.state.failed_downloads),
            "last_updated": self.state.last_updated.isoformat(),
        }
