"""Async-library compatibility patches and CLI logging setup for the CocoIndex pipeline.

When the pipeline runs via ``cocoindex update app.py`` (CLI mode), ``main.py``
is never imported, so the Python 3.14 patches applied there are absent.  This
module re-applies the critical patches so that LlamaIndex / httpx / httpcore
work correctly inside CocoIndex's Rust-backed async runtime.

Call :func:`apply_async_patches` once at module-load time in any CocoIndex
pipeline entry point that does not go through the FastAPI ``main.py``.
Call :func:`setup_cli_logging` from the CLI ``if __name__ == "__main__":``
block to mirror the file + console logging that the server uses.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

_PATCHED = False  # guard against double-patching


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def apply_async_patches() -> None:
    """Apply async-library compatibility patches (idempotent).

    Patches applied
    ---------------
    1. **anyio CancelScope** — ``__enter__`` / ``__exit__`` become no-ops when
       ``asyncio.current_task()`` is ``None`` (Python 3.14 executor-thread
       context).  Without this, ``WeakValueDictionary`` raises
       ``TypeError: cannot create weak reference to 'NoneType' object`` inside
       httpcore's ``AsyncShieldCancellation``.

    2. **httpcore AsyncShieldCancellation** — replaced with a safe wrapper that
       skips the anyio shield when there is no current task.  Also patches the
       class references already cached by httpcore's async sub-modules.

    3. **sniffio current_async_library** — accepts a *running* event loop even
       without a ``current_task``.  Prevents
       ``AsyncLibraryNotFoundError("unknown async library")`` inside LlamaIndex's
       anyio usage.

    4. **neo4j AsyncCooperativeRLock** — ``acquire`` / ``release`` use a
       sentinel object instead of raw ``None`` as the task identity when
       ``asyncio.current_task()`` returns ``None`` in CocoIndex's Rust runtime.
       Without this, the lock stores ``None`` as its owner and then ``release``
       checks ``self._owner is None`` → raises ``RuntimeError("Lock is not
       acquired.")`` even though the lock was properly entered.  Triggered every
       time neo4j checks ``pool.ssr_enabled`` during a session ``_connect``.

    5. **aiohttp TimerContext** — ``__enter__`` raises
       ``RuntimeError("Timeout context manager should be used inside a task")``
       when ``asyncio.current_task()`` is ``None``.  This is hit by the
       ``elasticsearch-py`` async client (which uses ``aiohttp``) whenever the
       Flexible search adapter writes chunks from inside CocoIndex's Rust
       coroutine dispatch.  Fix: when there is no current task, enter the context
       as a no-op (skipping the cancellation timer registration).

    6. **nest_asyncio.apply no-op** — ``nest_asyncio.apply()`` patches
       ``loop.run_until_complete()`` in a way that runs coroutines without an
       ``asyncio.Task`` wrapper.  This breaks ``asyncio.Runner.close()`` →
       ``shutdown_default_executor()`` which calls ``asyncio.timeout()`` (requires
       a Task on Python 3.14).  Fix: replace ``nest_asyncio.apply`` with a no-op
       so every caller (including third-party libraries) is silently ignored.

    7. **asyncio.wait_for safe wrapper** — On Python 3.14 ``asyncio.wait_for``
       uses ``asyncio.timeout()`` internally, which raises
       ``RuntimeError("Timeout should be used inside a task")`` when called
       outside a Task (e.g. neo4j bolt socket ``_connect_secure``).  Fix: when
       there is no current task and a timeout is set, fall back to
       ``asyncio.wait()`` which does not require a Task.

    Safe to call multiple times; subsequent calls are no-ops.
    """
    global _PATCHED
    if _PATCHED:
        return
    _PATCHED = True

    _patch_anyio_cancel_scope()
    _patch_httpcore_shield()
    _patch_sniffio()
    _patch_neo4j_cooperative_rlock()
    _patch_aiohttp_timer_context()
    _patch_nest_asyncio()
    _patch_asyncio_wait_for()


def setup_cli_logging(log_prefix: str = "flexible-graphrag-coco") -> str:
    """Set up file + console logging for CLI / standalone CocoIndex runs.

    Creates a timestamped log file in the current working directory, mirroring
    the file-handler setup in ``main.py``.  Returns the log-file path.

    Parameters
    ----------
    log_prefix:
        Prefix for the log filename (default: ``flexible-graphrag-coco``).
        The full name is ``{prefix}-{YYYYMMDD-HHMMSS}.log``.
    """
    from datetime import datetime  # local import — avoids global dependency

    log_level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    log_filename = f'{log_prefix}-{datetime.now().strftime("%Y%m%d-%H%M%S")}.log'

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(log_level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    # Avoid adding duplicate handlers if called more than once.
    _existing_files = {
        h.baseFilename for h in root.handlers if isinstance(h, logging.FileHandler)
    }
    if log_filename not in _existing_files:
        root.addHandler(file_handler)
    _has_stream = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
                      for h in root.handlers)
    if not _has_stream:
        root.addHandler(console_handler)

    root.setLevel(log_level)
    return log_filename


# ─────────────────────────────────────────────────────────────────────────────
# Internal patch helpers
# ─────────────────────────────────────────────────────────────────────────────

def _patch_anyio_cancel_scope() -> None:
    """Patch anyio.CancelScope for Python 3.14 / CocoIndex async runtime.

    anyio uses ``asyncio.current_task()`` in ``CancelScope.__enter__`` to look
    up the host task in a ``WeakValueDictionary``.  On Python 3.14 (and in
    CocoIndex's Rust-driven coroutine dispatch), ``current_task()`` can be
    ``None``, causing ``TypeError: cannot create weak reference to 'NoneType'``.

    Fix: when ``current_task()`` is ``None``, the scope enters as a no-op (just
    sets ``_active=True``).  Exit mirrors: if the scope was entered as a no-op,
    exit is also a no-op.  anyio 4.14+ uses ``__slots__`` so we track no-op
    scopes by ``id()`` in a module-level set.
    """
    try:
        from anyio._backends._asyncio import CancelScope as _CS  # type: ignore[import-untyped]
        _orig_enter = _CS.__enter__
        _orig_exit = _CS.__exit__
        _noop_ids: set = set()

        def _safe_enter(self):
            if asyncio.current_task() is None:
                self._active = True
                _noop_ids.add(id(self))
                return self
            return _orig_enter(self)

        def _safe_exit(self, exc_type, exc_val, exc_tb):
            _id = id(self)
            if _id in _noop_ids:
                self._active = False
                _noop_ids.discard(_id)
                return False
            return _orig_exit(self, exc_type, exc_val, exc_tb)

        _CS.__enter__ = _safe_enter
        _CS.__exit__ = _safe_exit
    except Exception:
        pass


def _patch_httpcore_shield() -> None:
    """Patch httpcore.AsyncShieldCancellation for Python 3.14 / CocoIndex.

    httpcore uses ``AsyncShieldCancellation`` (backed by an anyio CancelScope)
    during HTTP connection cleanup.  When ``current_task()`` is ``None`` the
    anyio scope raises a ``TypeError``.

    Fix: replace ``AsyncShieldCancellation`` with a wrapper that skips the
    anyio shield entirely when there is no current task.  The cached references
    in httpcore's async sub-modules are updated too.

    Guard: if main.py's ``_apply_python314_patches()`` has already installed its
    own safe wrapper (detected by the presence of ``_orig_AsyncShieldCancellation``
    on the module), skip this patch entirely.  Applying both wrappers creates a
    mutual-recursion chain because each wrapper's ``__init__`` calls the other's
    class, causing a ``RecursionError`` 540 frames deep.
    """
    try:
        import httpcore._synchronization as _sync  # type: ignore[import-untyped]

        # main.py already patched — skip to avoid double-patch recursion.
        if hasattr(_sync, "_orig_AsyncShieldCancellation"):
            return

        _orig = _sync.AsyncShieldCancellation

        class _SafeShield:
            def __init__(self) -> None:
                self._active = asyncio.current_task() is not None
                if self._active:
                    self._orig = _orig()

            def __enter__(self):
                if self._active:
                    self._orig.__enter__()
                return self

            def __exit__(self, *args):
                if self._active:
                    return self._orig.__exit__(*args)
                return False

        _sync._orig_AsyncShieldCancellation = _orig  # type: ignore[attr-defined]
        _sync.AsyncShieldCancellation = _SafeShield

        # Update references already cached at import time in async sub-modules.
        for _mod_name in (
            "httpcore._async.connection_pool",
            "httpcore._async.http11",
            "httpcore._async.http2",
        ):
            try:
                import importlib
                _m = importlib.import_module(_mod_name)
                if hasattr(_m, "AsyncShieldCancellation"):
                    _m.AsyncShieldCancellation = _SafeShield
            except Exception:
                pass
    except Exception:
        pass


def _patch_sniffio() -> None:
    """Patch sniffio.current_async_library for Python 3.14 / CocoIndex runtime.

    On Python 3.14 ``asyncio.current_task()`` returns *None* in threads
    spawned from an async context.  ``sniffio`` uses ``current_task()`` as its
    primary asyncio detection signal and therefore raises
    ``AsyncLibraryNotFoundError("unknown async library, or not in async context")``.

    The same problem occurs in CocoIndex's Rust-backed tokio runtime — the
    Python asyncio event loop *is* running (``get_running_loop()`` succeeds)
    but there may be no ``current_task``.

    Fix: also return ``"asyncio"`` when ``get_running_loop()`` succeeds.
    """
    try:
        import sniffio._impl as _sniffio_impl  # type: ignore[import-untyped]

        def _safe_current_async_library():
            # Fast path: context var or thread-local already set by anyio / trio.
            value = _sniffio_impl.thread_local.name
            if value is not None:
                return value
            value = _sniffio_impl.current_async_library_cvar.get()
            if value is not None:
                return value
            # Asyncio sniff: accept a running event loop even without a task.
            if "asyncio" in sys.modules:
                try:
                    if asyncio.current_task() is not None:
                        return "asyncio"
                    asyncio.get_running_loop()
                    return "asyncio"
                except RuntimeError:
                    pass
            raise _sniffio_impl.AsyncLibraryNotFoundError(
                "unknown async library, or not in async context"
            )

        _sniffio_impl.current_async_library = _safe_current_async_library
        import sniffio as _sniffio  # type: ignore[import-untyped]
        _sniffio.current_async_library = _safe_current_async_library
    except Exception:
        pass


def _patch_neo4j_cooperative_rlock() -> None:
    """Patch neo4j AsyncCooperativeRLock for Python 3.14 / CocoIndex runtime.

    ``AsyncCooperativeRLock`` uses ``asyncio.current_task()`` as the *owner
    identity* for reentrancy tracking.  In CocoIndex's Rust-backed coroutine
    dispatch, ``current_task()`` returns ``None`` even though an event loop is
    running.  This causes:

    * ``acquire()``  → stores ``None`` as ``self._owner``
    * ``release()``  → checks ``if self._owner is None: raise RuntimeError(...)``
      and incorrectly treats the stored ``None`` as "unlocked", raising
      ``RuntimeError("Lock is not acquired.")`` on every ``pool.ssr_enabled``
      call inside ``neo4j._async.work.workspace._connect``.

    Fix: both ``acquire`` and ``release`` (and ``__enter__``) substitute a
    module-level sentinel object for ``None`` whenever ``current_task()`` is
    ``None`` but a running event loop exists.  The sentinel is a stable
    singleton, so the "same context" identity check (``self._owner is me``)
    works correctly across acquire/release pairs within CocoIndex's runtime.
    """
    try:
        from neo4j._async_compat.concurrency import (  # type: ignore[import-untyped]
            AsyncCooperativeRLock,
        )

        # One singleton per patched runtime — represents the "taskless async
        # context" as a stable owner identity.
        _SENTINEL = object()

        def _current_me():
            task = asyncio.current_task()
            if task is not None:
                return task
            try:
                asyncio.get_running_loop()
                return _SENTINEL
            except RuntimeError:
                return None  # not in any event loop — leave as None

        def _safe_acquire(self):
            me = _current_me()
            if self._owner is None:
                self._owner = me
                self._count = 1
                return True
            if self._owner is me:
                self._count += 1
                return True
            raise RuntimeError("Cannot acquire a foreign locked cooperative lock.")

        def _safe_release(self):
            me = _current_me()
            if self._owner is None:
                raise RuntimeError("Lock is not acquired.")
            if self._owner is not me:
                raise RuntimeError("Cannot release a foreign lock.")
            self._count -= 1
            if not self._count:
                self._owner = None

        # Replace both `acquire` AND the class-level `__enter__ = acquire`
        # alias (the alias captured the original function at class-body time).
        AsyncCooperativeRLock.acquire = _safe_acquire   # type: ignore[method-assign]
        AsyncCooperativeRLock.__enter__ = _safe_acquire  # type: ignore[method-assign]
        AsyncCooperativeRLock.release = _safe_release    # type: ignore[method-assign]
        # __exit__ calls self.release() dynamically, so it picks up _safe_release
        # automatically — no need to replace it separately.
    except Exception:
        pass


def _patch_nest_asyncio() -> None:
    """Replace nest_asyncio.apply with a no-op on Python 3.14+.

    ``nest_asyncio.apply()`` patches ``loop.run_until_complete()`` to run
    coroutines *without* an ``asyncio.Task`` wrapper.  On Python 3.14 this
    breaks ``asyncio.Runner.close()`` → ``shutdown_default_executor()`` which
    uses ``asyncio.timeout()`` and requires a Task.  Replacing ``apply`` with
    a no-op is safe: it prevents the monkey-patch from ever taking effect, so
    ``run_until_complete`` keeps its standard Task-wrapping behaviour.
    """
    try:
        import nest_asyncio as _nest_asyncio  # type: ignore[import-untyped]
        _nest_asyncio.apply = lambda *a, **kw: None
    except ImportError:
        pass


def _patch_asyncio_wait_for() -> None:
    """Patch asyncio.wait_for for Python 3.14 / CocoIndex runtime.

    On Python 3.14 ``asyncio.wait_for`` uses ``asyncio.timeout()`` internally,
    which raises ``RuntimeError("Timeout should be used inside a task")`` when
    invoked without a current asyncio ``Task``.  The neo4j bolt socket
    ``_connect_secure`` method and other network code use ``wait_for`` with
    explicit timeouts for connection establishment.

    Fix: when ``asyncio.current_task()`` is ``None`` and a timeout is requested,
    fall back to ``asyncio.wait({task}, timeout=…)`` which does not require a
    Task object.
    """
    try:
        _orig_wait_for = asyncio.wait_for

        async def _safe_wait_for(fut: Any, timeout: Any, **kwargs: Any) -> Any:
            if timeout is None or asyncio.current_task() is not None:
                return await _orig_wait_for(fut, timeout, **kwargs)
            # No current Task — implement timeout with asyncio.wait() instead
            # of asyncio.timeout() (which requires a Task on Python 3.14).
            task = asyncio.ensure_future(fut)
            done, pending = await asyncio.wait({task}, timeout=timeout)
            if pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                raise asyncio.TimeoutError()
            return task.result()

        asyncio.wait_for = _safe_wait_for  # type: ignore[assignment]
    except Exception:
        pass


def _patch_aiohttp_timer_context() -> None:
    """Patch aiohttp.helpers.TimerContext for Python 3.14 / CocoIndex runtime.

    ``TimerContext.__enter__`` hard-raises
    ``RuntimeError("Timeout context manager should be used inside a task")``
    when ``asyncio.current_task()`` returns ``None``.  This blocks the
    ``elasticsearch-py`` async client (which uses ``aiohttp`` internally) from
    connecting when driven from CocoIndex's Rust coroutine dispatch, because
    there is no asyncio ``Task`` wrapper around the coroutine.

    Fix: when there is no current task but an event loop IS running, silently
    skip the timer registration (treat it as a no-op enter).  The actual HTTP
    timeout is not lost — ``aiohttp``'s connector also uses lower-level socket
    timeouts, so requests will not hang indefinitely.
    """
    try:
        import aiohttp.helpers as _aiohttp_helpers  # type: ignore[import-untyped]

        _OrigTimerContext = _aiohttp_helpers.TimerContext
        _orig_enter = _OrigTimerContext.__enter__

        def _safe_timer_enter(self: Any) -> Any:
            if asyncio.current_task() is None:
                try:
                    asyncio.get_running_loop()
                    # Taskless context — skip cancellation registration, return self
                    return self
                except RuntimeError:
                    pass
            return _orig_enter(self)

        _OrigTimerContext.__enter__ = _safe_timer_enter  # type: ignore[method-assign]
    except Exception:
        pass
