#!/usr/bin/env python3
"""Bounded, read-only browser snapshot of a public training dashboard.

The capture records one browser page state. It does not log in, submit forms,
or call undocumented routes directly. Only GET/HEAD requests to the origin and
explicitly allowed hosts are permitted; WebSockets require their own allowlist.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

DEFAULT_MAX_BYTES = 200 * 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_WAIT_SECONDS = 120.0
MAX_ACTIONS = 20
MAX_LOG_ENTRIES = 5000
MAX_LOG_BYTES = 8 * 1024 * 1024
MAX_SCREENSHOT_PIXELS = 32_000_000
MAX_RESPONSE_ENTRIES = 5000
MAX_DISTINCT_GET_URLS = 5000
MAX_GETS_PER_URL = 4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_host(value: str) -> str:
    """Return a hostname from a bare host or URL; reject paths and credentials."""
    candidate = value if "://" in value else "//" + value
    parsed = urlsplit(candidate)
    if parsed.username or parsed.password or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError(f"Expected a bare hostname, got {value!r}")
    if not parsed.hostname:
        raise ValueError(f"Missing hostname in {value!r}")
    return parsed.hostname.encode("idna").decode("ascii").lower().rstrip(".")


def validate_url(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("--url must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password:
        raise ValueError("Credentials in --url are not allowed")
    host = parsed.hostname.encode("idna").decode("ascii").lower().rstrip(".")
    return host, parsed.scheme


def blocked_path_reason(url: str) -> str | None:
    """Block obvious analytics and refresh endpoints while allowing data APIs."""
    parts = urlsplit(url)
    segments = {segment.lower() for segment in parts.path.split("/") if segment}
    if segments & {"analytics", "tracking", "track", "beacon"}:
        return "analytics route blocked"
    if "refresh" in segments:
        return "refresh route blocked"
    return None


class SnapshotWriter:
    """Create immutable evidence files under one fresh output directory."""

    def __init__(self, output: Path, max_bytes: int) -> None:
        self.output = Path(output)
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self.entries: list[dict[str, Any]] = []
        self.output.parent.mkdir(parents=True, exist_ok=True)
        # A capture never resumes into or overwrites an existing snapshot.
        self.output.mkdir(exist_ok=False)
        (self.output / "raw").mkdir()
        self.manifest: dict[str, Any] = {
            "schema_version": 1,
            "created_at": utc_now(),
            "capture_status": "running",
            "run_error": None,
            "url": None,
            "allowed_hosts": [],
            "allowed_websocket_hosts": [],
            "max_bytes": max_bytes,
            "entries": self.entries,
        }
        self.flush_manifest()

    @staticmethod
    def safe_relative_path(relative: str) -> PurePosixPath:
        path = PurePosixPath(relative)
        if path.is_absolute() or not path.parts or any(part in ("", ".", "..") for part in path.parts):
            raise ValueError(f"Unsafe evidence path: {relative!r}")
        return path

    def add_skipped(
        self,
        *,
        kind: str,
        url: str | None,
        status: int | None,
        reason: str,
        at: str | None = None,
    ) -> dict[str, Any]:
        entry = {
            "kind": kind,
            "url": url,
            "status": status,
            "application_status": "skipped",
            "reason": reason,
            "path": None,
            "bytes": 0,
            "sha256": None,
            "retrieved_at": at or utc_now(),
        }
        self.entries.append(entry)
        return entry

    def save(
        self,
        *,
        relative: str,
        body: bytes,
        kind: str,
        url: str | None,
        status: int | None = None,
        content_type: str | None = None,
        at: str | None = None,
    ) -> dict[str, Any]:
        safe_rel = self.safe_relative_path(relative)
        if len(body) > self.max_bytes - self.used_bytes:
            return self.add_skipped(
                kind=kind,
                url=url,
                status=status,
                reason="total capture byte budget exhausted",
                at=at,
            )
        target = self.output.joinpath(*safe_rel.parts)
        resolved_root = self.output.resolve()
        if not target.resolve().is_relative_to(resolved_root):
            raise ValueError(f"Evidence path escapes output directory: {relative!r}")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation makes accidental replacement impossible.
        with target.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        entry = {
            "kind": kind,
            "url": url,
            "status": status,
            "application_status": "captured",
            "content_type": content_type,
            "path": safe_rel.as_posix(),
            "bytes": len(body),
            "sha256": sha256_bytes(body),
            "retrieved_at": at or utc_now(),
        }
        self.used_bytes += len(body)
        self.entries.append(entry)
        return entry

    def flush_manifest(self) -> None:
        tmp = self.output / ".manifest.json.tmp"
        data = json.dumps(self.manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        with tmp.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, self.output / "manifest.json")


class DashboardCapture:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        origin, _scheme = validate_url(args.url)
        self.allowed_hosts = {origin, *(normalized_host(host) for host in args.allow_host)}
        self.allowed_ws_hosts = {normalized_host(host) for host in args.allow_websocket_host}
        self.writer = SnapshotWriter(args.output, args.max_bytes)
        self.writer.manifest.update({
            "url": args.url,
            "allowed_hosts": sorted(self.allowed_hosts),
            "allowed_websocket_hosts": sorted(self.allowed_ws_hosts),
            "max_response_bytes": args.max_response_bytes,
            "max_screenshot_pixels": MAX_SCREENSHOT_PIXELS,
            "wait_seconds": args.wait_seconds,
            "click_selectors": list(args.click_selector),
            "scroll_selector": args.scroll_selector,
            "max_scroll_shots": args.max_scroll_shots,
            "websocket_frame_limit_bytes": min(args.max_response_bytes, args.max_bytes),
        })
        self.response_buffered_bytes = 0
        self.responses: list[tuple[dict[str, Any], bytes]] = []
        self.get_url_counts: dict[str, int] = {}
        self.response_entries_dropped = 0
        self.blocked_requests: list[dict[str, Any]] = []
        self.console: list[dict[str, Any]] = []
        self.console_bytes = 0
        self.dropped_log_entries = 0
        self.page_errors: list[dict[str, Any]] = []
        self.failed_requests: list[dict[str, Any]] = []
        self.selector_actions: list[dict[str, Any]] = []
        self.scroll_capture: dict[str, Any] | None = None
        self.websocket_lines: list[bytes] = []
        self.websocket_bytes = 0
        self.websocket_dropped = 0

    def _bounded_log(self, target: list[dict[str, Any]], record: dict[str, Any]) -> None:
        encoded_size = len(json.dumps(record, ensure_ascii=False).encode("utf-8")) + 1
        if len(target) >= MAX_LOG_ENTRIES or self.console_bytes + encoded_size > MAX_LOG_BYTES:
            self.dropped_log_entries += 1
            return
        target.append(record)
        self.console_bytes += encoded_size

    def _capture_response(self, response: Any) -> None:
        request = response.request
        if request.is_navigation_request():
            return
        if len(self.responses) >= MAX_RESPONSE_ENTRIES:
            self.response_entries_dropped += 1
            return
        url = response.url
        status = response.status
        at = utc_now()
        record: dict[str, Any] = {
            "kind": "http_response",
            "url": url,
            "status": status,
            "application_status": "pending_body",
            "method": request.method,
            "resource_type": request.resource_type,
            "retrieved_at": at,
            "path": None,
            "bytes": 0,
            "sha256": None,
            "content_type": response.headers.get("content-type"),
        }
        try:
            # Browser APIs can expose decoded bytes while Content-Length is
            # the compressed wire size, so enforce the cap on the body itself.
            body = response.body()
        except Exception as error:
            record["application_status"] = "body_read_failed"
            record["reason"] = f"{type(error).__name__}: {error}"
            self.responses.append((record, b""))
            return
        if len(body) > self.args.max_response_bytes:
            record["application_status"] = "skipped_response_limit"
            record["reason"] = f"body exceeds {self.args.max_response_bytes} bytes"
            self.responses.append((record, b""))
            return
        if len(body) > self.args.max_bytes - self.response_buffered_bytes:
            record["application_status"] = "skipped_response_buffer_limit"
            record["reason"] = "response bodies reached total capture byte budget"
            self.responses.append((record, b""))
            return
        self.response_buffered_bytes += len(body)
        self.responses.append((record, body))

    def _capture_websocket(self, url: str, message: str | bytes) -> None:
        at = utc_now()
        is_binary = isinstance(message, bytes)
        frame_limit = min(self.args.max_response_bytes, self.args.max_bytes)
        if len(message) > frame_limit - self.websocket_bytes:
            self.websocket_dropped += 1
            return
        payload = base64.b64encode(message).decode("ascii") if is_binary else message
        record = {
            "url": url,
            "received_at": at,
            "payload_bytes": len(message) if is_binary else len(message.encode("utf-8")),
            "encoding": "base64" if is_binary else "utf-8",
            "payload": payload,
        }
        line = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        if len(line) > frame_limit - self.websocket_bytes:
            self.websocket_dropped += 1
            return
        self.websocket_lines.append(line)
        self.websocket_bytes += len(line)

    def run(self) -> int:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:
            raise RuntimeError("Playwright is required; install research/public-networks/capture-requirements.txt") from error

        browser = None
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=True,
                    executable_path=self.args.chromium_path,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                self.writer.manifest["capture_code_sha256"] = sha256_file(Path(__file__))
                self.writer.manifest["browser_version"] = browser.version
                self.writer.manifest["max_response_entries"] = MAX_RESPONSE_ENTRIES
                self.writer.manifest["max_distinct_get_urls"] = MAX_DISTINCT_GET_URLS
                self.writer.manifest["max_gets_per_url"] = MAX_GETS_PER_URL
                context = browser.new_context(
                    service_workers="block",
                    accept_downloads=False,
                    viewport={"width": 1440, "height": 1000},
                )
                context.route("**/*", self._route_request)
                context.route_web_socket("**/*", self._route_websocket)
                page = context.new_page()
                page.on("response", self._capture_response)
                page.on("console", self._capture_console)
                page.on("pageerror", self._capture_page_error)
                page.on("requestfailed", self._capture_failed_request)

                navigation_response = None
                try:
                    navigation_response = page.goto(self.args.url, wait_until="domcontentloaded", timeout=45000)
                except Exception as error:
                    self._bounded_log(self.page_errors, {"at": utc_now(), "error": f"navigation: {type(error).__name__}: {error}"})
                if navigation_response is not None:
                    self._capture_initial_html(navigation_response)
                page.wait_for_timeout(round(self.args.wait_seconds * 1000))

                for selector in self.args.click_selector:
                    action = {"selector": selector, "attempted_at": utc_now()}
                    try:
                        locator = page.locator(selector).first
                        locator.wait_for(state="visible", timeout=10000)
                        locator.click(timeout=10000)
                        action.update({"status": "clicked", "completed_at": utc_now()})
                        page.wait_for_timeout(round(self.args.wait_seconds * 1000))
                    except Exception as error:
                        action.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
                    self.selector_actions.append(action)

                self._capture_final_page(page)
                context.close()
                browser.close()
                browser = None
        except Exception as error:
            self.writer.manifest["capture_status"] = "failed"
            self.writer.manifest["run_error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            if browser is not None:
                # Exiting sync_playwright() owns cleanup after exceptions; do
                # not call browser.close() after its event loop has stopped.
                browser = None
            for finalize in (self._flush_responses, self._save_browser_logs, self._save_websocket_frames):
                try:
                    finalize()
                except Exception as error:
                    self._bounded_log(self.page_errors, {"at": utc_now(), "error": f"finalization: {type(error).__name__}: {error}"})
            has_capture_issues = (
                bool(self.page_errors or self.failed_requests)
                or any(action.get("status") == "failed" for action in self.selector_actions)
                or any(message.get("type") == "error" for message in self.console)
                or any(
                    entry.get("application_status") != "captured"
                    or (isinstance(entry.get("status"), int) and entry["status"] >= 400)
                    for entry in self.writer.entries
                )
            )
            self.writer.manifest.update({
                "capture_status": self.writer.manifest.get("capture_status") if self.writer.manifest.get("capture_status") == "failed" else ("complete_with_errors" if has_capture_issues else "complete"),
                "completed_at": utc_now(),
                "selector_actions": self.selector_actions,
                "scroll_capture": self.scroll_capture,
                "blocked_requests": self.blocked_requests,
                "dropped_browser_log_entries": self.dropped_log_entries,
                "websocket_frames_dropped": self.websocket_dropped,
                "response_entries_dropped": self.response_entries_dropped,
                "total_saved_bytes": self.writer.used_bytes,
            })
            self.writer.flush_manifest()
        return 0

    def _route_request(self, route: Any) -> None:
        request = route.request
        url = request.url
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower().rstrip(".")
        reason = None
        if parsed.scheme not in ("http", "https"):
            reason = "non-HTTP request blocked"
        elif request.method.upper() not in {"GET", "HEAD"}:
            reason = f"{request.method.upper()} request blocked; only GET/HEAD allowed"
        elif host not in self.allowed_hosts:
            reason = "host not allowlisted"
        else:
            reason = blocked_path_reason(url)
        if reason is None and request.method.upper() == "GET":
            current_count = self.get_url_counts.get(url, 0)
            if current_count >= MAX_GETS_PER_URL:
                reason = f"GET repeat cap ({MAX_GETS_PER_URL}) reached for URL"
            elif current_count == 0 and len(self.get_url_counts) >= MAX_DISTINCT_GET_URLS:
                reason = "distinct GET request safety cap reached"
            elif len(url) > 8192:
                reason = "request URL exceeds 8192-character safety limit"
            else:
                self.get_url_counts[url] = current_count + 1
        if reason:
            record = {"url": url, "method": request.method, "reason": reason, "at": utc_now()}
            record["url"] = record["url"][:4000]
            self._bounded_log(self.blocked_requests, record)
            route.abort("blockedbyclient")
            return
        route.continue_()

    def _route_websocket(self, route: Any) -> None:
        url = route.url
        host = (urlsplit(url).hostname or "").lower().rstrip(".")
        if host not in self.allowed_ws_hosts:
            self._bounded_log(self.blocked_requests, {
                "url": url[:4000],
                "method": "WEBSOCKET",
                "reason": "WebSocket host not explicitly allowlisted",
                "at": utc_now(),
            })
            route.close(code=1008, reason="WebSocket host not allowlisted")
            return
        server = route.connect_to_server()
        route.on_message(lambda message: server.send(message))
        server.on_message(lambda message: (self._capture_websocket(url, message), route.send(message)))

    def _capture_initial_html(self, response: Any) -> None:
        at = utc_now()
        try:
            body = response.body()
        except Exception as error:
            self.writer.add_skipped(
                kind="initial_html",
                url=response.url,
                status=response.status,
                reason=f"document body read failed: {type(error).__name__}: {error}",
                at=at,
            )
            return
        content_type = response.headers.get("content-type")
        if len(body) > self.args.max_response_bytes:
            self.writer.add_skipped(
                kind="initial_html",
                url=response.url,
                status=response.status,
                reason=f"document exceeds {self.args.max_response_bytes} byte per-response limit",
                at=at,
            )
            return
        self.writer.save(
            relative="artifacts/initial.html",
            body=body,
            kind="initial_html",
            url=response.url,
            status=response.status,
            content_type=content_type,
            at=at,
        )
        self.writer.flush_manifest()

    def _save_artifact(self, name: str, content: bytes, kind: str, url: str | None, content_type: str) -> dict[str, Any]:
        if not content:
            return self.writer.add_skipped(kind=kind, url=url, status=None, reason="generated artifact was empty")
        if len(content) > self.args.max_bytes:
            return self.writer.add_skipped(
                kind=kind,
                url=url,
                status=None,
                reason="artifact exceeds total capture byte budget",
            )
        return self.writer.save(
            relative=f"artifacts/{name}",
            body=content,
            kind=kind,
            url=url,
            status=None,
            content_type=content_type,
        )

    def _capture_final_page(self, page: Any) -> None:
        current_url = page.url or self.args.url
        try:
            dom = page.content().encode("utf-8")
            if len(dom) <= self.args.max_response_bytes:
                self._save_artifact("rendered_dom.html", dom, "rendered_dom", current_url, "text/html; charset=utf-8")
            else:
                self.writer.add_skipped(kind="rendered_dom", url=current_url, status=None, reason="DOM exceeds per-response limit")
        except Exception as error:
            self.writer.add_skipped(kind="rendered_dom", url=current_url, status=None, reason=f"{type(error).__name__}: {error}")
        try:
            title = page.title()
            text = page.locator("body").inner_text(timeout=5000)
            summary = json.dumps({"url": current_url, "title": title, "visible_text": text[:1_000_000]}, ensure_ascii=False, indent=2).encode("utf-8")
            self._save_artifact("page_summary.json", summary, "page_summary", current_url, "application/json")
        except Exception as error:
            self.writer.add_skipped(kind="page_summary", url=current_url, status=None, reason=f"{type(error).__name__}: {error}")
        try:
            dimensions = page.evaluate("() => ({width: document.documentElement.scrollWidth, height: document.documentElement.scrollHeight})")
            pixels = int(dimensions["width"]) * int(dimensions["height"])
            if pixels > MAX_SCREENSHOT_PIXELS:
                self.writer.add_skipped(kind="screenshot_png", url=current_url, status=None, reason=f"page area {pixels} pixels exceeds {MAX_SCREENSHOT_PIXELS} pixel safety limit")
            else:
                screenshot = page.screenshot(full_page=True, animations="disabled", timeout=30000)
                self._save_artifact("full_page.png", screenshot, "screenshot_png", current_url, "image/png")
        except Exception as error:
            self.writer.add_skipped(kind="screenshot_png", url=current_url, status=None, reason=f"{type(error).__name__}: {error}")
        if self.args.scroll_selector:
            self._capture_scroll_tiles(page, current_url)
        try:
            pdf = page.pdf(print_background=True, prefer_css_page_size=True)
            if len(pdf) > self.args.max_response_bytes:
                self.writer.add_skipped(kind="page_pdf", url=current_url, status=None, reason="PDF exceeds per-artifact size limit")
            else:
                self._save_artifact("page.pdf", pdf, "page_pdf", current_url, "application/pdf")
        except Exception as error:
            self.writer.add_skipped(kind="page_pdf", url=current_url, status=None, reason=f"{type(error).__name__}: {error}")
        try:
            cdp = page.context.new_cdp_session(page)
            cdp.send("Page.enable")
            snapshot = cdp.send("Page.captureSnapshot", {"format": "mhtml"})
            mhtml = snapshot["data"].encode("utf-8")
            if len(mhtml) > self.args.max_response_bytes:
                self.writer.add_skipped(kind="page_mhtml", url=current_url, status=None, reason="MHTML exceeds per-artifact size limit")
            else:
                self._save_artifact("page.mhtml", mhtml, "page_mhtml", current_url, "multipart/related")
            cdp.detach()
        except Exception as error:
            self.writer.add_skipped(kind="page_mhtml", url=current_url, status=None, reason=f"{type(error).__name__}: {error}")
        self.writer.flush_manifest()

    def _capture_scroll_tiles(self, page: Any, current_url: str) -> None:
        selector = self.args.scroll_selector
        record: dict[str, Any] = {"selector": selector, "tiles": []}
        pane = None
        original = None
        try:
            pane = page.locator(f":is({selector}):visible").first
            pane.wait_for(state="visible", timeout=5000)
            original = pane.evaluate("element => element.scrollTop")
            dimensions = pane.evaluate("element => ({clientHeight: element.clientHeight, scrollHeight: element.scrollHeight})")
            viewport = int(dimensions["clientHeight"])
            maximum = max(0, int(dimensions["scrollHeight"]) - viewport)
            step = max(1, int(viewport * 0.8))
            positions = list(range(0, maximum + 1, step))[: self.args.max_scroll_shots]
            if maximum and (not positions or positions[-1] != maximum) and len(positions) < self.args.max_scroll_shots:
                positions.append(maximum)
            for index, scroll_top in enumerate(positions, 1):
                pane.evaluate("(element, top) => { element.scrollTop = top; }", scroll_top)
                page.wait_for_timeout(150)
                image = page.screenshot(full_page=False, animations="disabled", timeout=30000)
                if len(image) > self.args.max_response_bytes:
                    self.writer.add_skipped(kind="scroll_pane_screenshot", url=current_url, status=None, reason="screenshot tile exceeds per-artifact size limit")
                    break
                name = f"scroll_tile_{index:02d}.png"
                entry = self.writer.save(
                    relative=f"artifacts/{name}",
                    body=image,
                    kind="scroll_pane_screenshot",
                    url=current_url,
                    status=None,
                    content_type="image/png",
                )
                entry["scroll_selector"] = selector
                entry["scroll_top"] = int(scroll_top)
                record["tiles"].append({"path": entry.get("path"), "scroll_top": int(scroll_top), "status": entry.get("application_status")})
                if entry.get("application_status") == "skipped":
                    break
            record.update({"status": "captured", "original_scroll_top": int(original), "client_height": viewport, "scroll_height": int(dimensions["scrollHeight"])})
        except Exception as error:
            record.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
            self.writer.add_skipped(kind="scroll_pane_screenshot", url=current_url, status=None, reason=record["error"])
        finally:
            if pane is not None and original is not None:
                try:
                    pane.evaluate("(element, top) => { element.scrollTop = top; }", original)
                    page.wait_for_timeout(100)
                except Exception as error:
                    record["restore_error"] = f"{type(error).__name__}: {error}"
        self.scroll_capture = record

    def _capture_console(self, message: Any) -> None:
        record = {"at": utc_now(), "type": message.type, "text": message.text}
        self._bounded_log(self.console, record)

    def _capture_page_error(self, error: Any) -> None:
        self._bounded_log(self.page_errors, {"at": utc_now(), "error": str(error)[:4000]})

    def _capture_failed_request(self, request: Any) -> None:
        record = {
            "at": utc_now(),
            "url": request.url,
            "method": request.method,
            "failure": (request.failure or "request failed")[:2000],
        }
        record["url"] = record["url"][:4000]
        self._bounded_log(self.failed_requests, record)

    def _flush_responses(self) -> None:
        for index, (record, body) in enumerate(self.responses):
            if record.get("application_status") != "pending_body":
                self.writer.entries.append(record)
                continue
            if len(body) > self.args.max_bytes - self.writer.used_bytes:
                record["application_status"] = "skipped_total_budget"
                record["reason"] = "total capture byte budget exhausted"
                self.writer.entries.append(record)
                continue
            parsed = urlsplit(record["url"])
            extension = ".json" if "json" in (record.get("content_type") or "").lower() else ".body"
            name = hashlib.sha256(record["url"].encode("utf-8")).hexdigest()[:20]
            relative = f"raw/response_{name}_{index}{extension}"
            saved = self.writer.save(
                relative=relative,
                body=body,
                kind=record["kind"],
                url=record["url"],
                status=record["status"],
                content_type=record.get("content_type"),
                at=record["retrieved_at"],
            )
            saved["method"] = record["method"]
            saved["resource_type"] = record["resource_type"]
        self.responses.clear()
        self.writer.flush_manifest()

    def _save_browser_logs(self) -> None:
        logs = {
            "console": self.console,
            "page_errors": self.page_errors,
            "failed_requests": self.failed_requests,
            "blocked_requests": self.blocked_requests,
            "dropped_browser_log_entries": self.dropped_log_entries,
        }
        body = json.dumps(logs, ensure_ascii=False, indent=2).encode("utf-8")
        self._save_artifact("browser_logs.json", body, "browser_logs", self.args.url, "application/json")

    def _save_websocket_frames(self) -> None:
        if not self.websocket_lines and not self.websocket_dropped:
            return
        body = b"".join(self.websocket_lines)
        entry = self._save_artifact("websocket_frames.jsonl", body, "websocket_frames", self.args.url, "application/x-ndjson")
        entry["dropped_frames"] = self.websocket_dropped


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Public dashboard URL to open")
    parser.add_argument("--output", required=True, type=Path, help="Fresh, nonexistent output directory")
    parser.add_argument("--allow-host", action="append", default=[], help="Additional HTTP(S) hostname; repeat as needed")
    parser.add_argument("--allow-websocket-host", action="append", default=[], help="Explicit WebSocket hostname; repeat as needed")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES, help="Total saved payload/artifact cap (default: 200 MiB)")
    parser.add_argument("--max-response-bytes", type=int, default=DEFAULT_MAX_RESPONSE_BYTES, help="Maximum bytes for one response/document (default: 16 MiB)")
    parser.add_argument("--wait-seconds", type=float, default=12.0, help="Wait after DOMContentLoaded and each selector click (0-120; default: 12)")
    parser.add_argument("--click-selector", action="append", default=[], help="Click one visible CSS selector in order; repeat for ordered actions")
    parser.add_argument("--scroll-selector", help="Optional scrollable CSS selector for viewport screenshot tiles")
    parser.add_argument("--max-scroll-shots", type=int, default=12, help="Maximum scroll-pane viewport tiles (default: 12)")
    parser.add_argument("--chromium-path", default="/usr/sbin/chromium", help="System Chromium executable")
    return parser


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    try:
        validate_url(args.url)
        for host in args.allow_host + args.allow_websocket_host:
            normalized_host(host)
    except ValueError as error:
        parser.error(str(error))
    if args.max_bytes < 1 or args.max_bytes > 2 * 1024**3:
        parser.error("--max-bytes must be between 1 and 2 GiB")
    if args.max_response_bytes < 1:
        parser.error("--max-response-bytes must be positive")
    if not math.isfinite(args.wait_seconds) or not 0 <= args.wait_seconds <= MAX_WAIT_SECONDS:
        parser.error(f"--wait-seconds must be between 0 and {MAX_WAIT_SECONDS}")
    if len(args.click_selector) > MAX_ACTIONS:
        parser.error(f"At most {MAX_ACTIONS} --click-selector actions are allowed")
    if not 1 <= args.max_scroll_shots <= 48:
        parser.error("--max-scroll-shots must be between 1 and 48")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(args, parser)
    try:
        capture = DashboardCapture(args)
        return capture.run()
    except Exception as error:
        print(f"ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
