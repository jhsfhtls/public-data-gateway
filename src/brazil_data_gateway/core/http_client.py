"""Resilient HTTP client with Token Bucket, Circuit Breaker, and Stale-While-Error fallback."""

import json
import threading
import time
from typing import Any

import httpx

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.storage import HybridStorageEngine

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open and no stale cache fallback is available."""


class TokenBucketRateLimiter:
    """Thread-safe token bucket rate limiter to adhere to public API thresholds."""

    def __init__(self, capacity: float = 10.0, refill_rate: float = 2.0) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0, block: bool = True, timeout: float = 10.0) -> bool:
        """Acquires tokens from bucket, optionally blocking until replenished."""
        deadline = time.time() + timeout
        while True:
            with self._lock:
                now = time.time()
                elapsed = now - self.last_refill
                self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
                self.last_refill = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

            if not block or time.time() >= deadline:
                return False
            time.sleep(0.05)


class CircuitBreaker:
    """Circuit breaker guarding against cascading remote failures."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_failure_time = 0.0
        self._lock = threading.Lock()

    def can_execute(self) -> bool:
        """Check whether a request is allowed to attempt execution."""
        with self._lock:
            now = time.time()
            if self.state == "OPEN":
                if now - self.last_failure_time >= self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            return True

    def record_success(self) -> None:
        """Reset breaker to healthy CLOSED state."""
        with self._lock:
            self.failures = 0
            self.state = "CLOSED"

    def record_failure(self) -> None:
        """Record a failure and trip to OPEN state if threshold reached."""
        with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "OPEN"


class ResilientHttpClient:
    """Context-managed HTTP client with exponential backoff, caching, and circuit breaker."""

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        storage_engine: HybridStorageEngine | None = None,
        timeout: float = 30.0,
        rate_limit_per_sec: float = 5.0,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.storage_engine = storage_engine
        self.timeout = timeout
        self.rate_limiter = TokenBucketRateLimiter(
            capacity=max(rate_limit_per_sec * 2, 5.0),
            refill_rate=rate_limit_per_sec,
        )
        self.circuit_breaker = CircuitBreaker()
        self._client = httpx.Client(
            timeout=self.timeout,
            headers={"User-Agent": self.settings.user_agent},
        )
        self._flight_locks: dict[str, threading.Lock] = {}
        self._master_lock = threading.Lock()

    @property
    def client(self) -> httpx.Client:
        """Return active httpx client, re-opening if closed."""
        if self._client.is_closed:
            self._client = httpx.Client(
                timeout=self.timeout,
                headers={"User-Agent": self.settings.user_agent},
            )
        return self._client

    def __enter__(self) -> "ResilientHttpClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close underlying httpx client."""
        self._client.close()

    def _get_flight_lock(self, key: str) -> threading.Lock:
        with self._master_lock:
            if key not in self._flight_locks:
                self._flight_locks[key] = threading.Lock()
            return self._flight_locks[key]

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cache_ttl: int | None = None,
        stale_while_error: bool = True,
        max_retries: int = 3,
    ) -> httpx.Response:
        """Perform resilient GET request with caching, single-flight locking, and BCP fallback."""
        if params:
            cache_key = f"GET:{url}:{json.dumps(params, sort_keys=True)}"
        else:
            cache_key = f"GET:{url}"

        # 1. Check valid cache
        if cache_ttl is not None and self.storage_engine is not None:
            cached = self.storage_engine.get_cached(cache_key)
            if cached is not None:
                return httpx.Response(
                    status_code=cached["status_code"],
                    content=cached["value"],
                    headers={"content-type": cached["content_type"], "X-BDG-Cache": "HIT"},
                )

        # 2. Check Circuit Breaker
        if not self.circuit_breaker.can_execute():
            if stale_while_error and self.storage_engine is not None:
                stale = self.storage_engine.get_stale(cache_key)
                if stale is not None:
                    return httpx.Response(
                        status_code=stale["status_code"],
                        content=stale["value"],
                        headers={
                            "content-type": stale["content_type"],
                            "X-BDG-Stale": "true",
                            "X-BDG-Cache": "STALE",
                        },
                    )
            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN for {url}; downstream service is unavailable."
            )

        # 3. Single flight lock per cache key
        flight_lock = self._get_flight_lock(cache_key)
        with flight_lock:
            # Re-check cache in case peer flight populated it
            if cache_ttl is not None and self.storage_engine is not None:
                cached = self.storage_engine.get_cached(cache_key)
                if cached is not None:
                    return httpx.Response(
                        status_code=cached["status_code"],
                        content=cached["value"],
                        headers={"content-type": cached["content_type"], "X-BDG-Cache": "HIT"},
                    )

            last_error: Exception | None = None
            for attempt in range(max_retries + 1):
                self.rate_limiter.acquire(tokens=1.0)
                try:
                    resp = self.client.get(url, params=params, headers=headers)
                    if resp.status_code in _RETRYABLE_STATUS_CODES:
                        last_error = httpx.HTTPStatusError(
                            f"Server returned retryable status {resp.status_code}",
                            request=resp.request,
                            response=resp,
                        )
                        time.sleep(0.05 * (2**attempt))
                        continue

                    # Success: record in circuit breaker and cache
                    self.circuit_breaker.record_success()
                    if cache_ttl is not None and self.storage_engine is not None:
                        content_type = resp.headers.get("content-type", "application/json")
                        self.storage_engine.set_cached(
                            cache_key,
                            resp.content,
                            ttl_seconds=cache_ttl,
                            content_type=content_type,
                            status_code=resp.status_code,
                        )
                    return resp
                except (httpx.TransportError, httpx.TimeoutException) as err:
                    last_error = err
                    time.sleep(0.05 * (2**attempt))

            # Retries exhausted: record failure
            self.circuit_breaker.record_failure()

            # Attempt Stale fallback
            if stale_while_error and self.storage_engine is not None:
                stale = self.storage_engine.get_stale(cache_key)
                if stale is not None:
                    return httpx.Response(
                        status_code=stale["status_code"],
                        content=stale["value"],
                        headers={
                            "content-type": stale["content_type"],
                            "X-BDG-Stale": "true",
                            "X-BDG-Cache": "STALE",
                        },
                    )

            if last_error:
                raise last_error
            raise RuntimeError(f"Request to {url} failed with no specific error.")
