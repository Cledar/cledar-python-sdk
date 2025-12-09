import json
import re
import time
from collections.abc import Callable
from dataclasses import asdict
from threading import Thread
from typing import Any

from redis import ConnectionError as RedisConnectionError
from redis import Redis

from .logger import logger
from .model import ConfigAbstract as T

KEY_EVENT_FORMAT = "__keyspace@{DB}__:{KEY}"
KEY_EVENT_REGEX = r"__keyspace@(?P<db>\d+)__:(?P<key>.+)"

OP_EVENT_FORMAT = "__keyevent@{DB}__:{OPERATION}"


class RedisConfigStore:
    TYPE_NONE = "none"
    TYPE_LIST = "list"
    TYPE_STRING = "string"

    EVENT_DELETE = "del"
    EVENT_SET = "set"
    EVENT_RPUSH = "rpush"
    EVENT_LSET = "lset"

    def __init__(self, redis: Redis, prefix: str | None = None) -> None:
        self._redis: Redis = redis
        self._pubsub = redis.pubsub()  # type: ignore
        self._db: int = redis.connection_pool.connection_kwargs.get("db")
        self._prefix: str = prefix or ""
        self._cache: dict[str, str] = {}
        self._cache_verisons: dict[str, int] = {}
        self._monitoring: dict[
            str, list[Callable[[int, str, str, str], None] | None]
        ] = {}
        self._watcher_thread: Thread = Thread(target=self._watcher)
        self._watcher_thread.start()

    def is_ready(self) -> bool:
        try:
            return self._redis.ping()  # type: ignore
        except RedisConnectionError:
            return False

    def versions(self, key: str) -> int | None:
        return self._key_versions(key)

    def cached_version(self, key: str) -> int | None:
        return self._cache_verisons.get(key)

    def fetch(self, cls: type[T], key: str) -> T | None:
        if key not in self._cache:
            new_value = self._key_fetch(key)
            if new_value is None:
                return None
            self._cache[key] = new_value
            self._cache_verisons[key] = self._key_versions(key) or -1
            self._key_watch(key)
        return cls(**json.loads(self._cache[key]))

    def update(self, key: str, value: T) -> None:
        self._cache[key] = self._key_update(key, value)
        self._cache_verisons[key] = self._key_versions(key) or -1
        self._key_watch(key)

    def delete(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]
            del self._cache_verisons[key]
        self._key_delete(key)
        self._key_watch(key)

    def watch(
        self, key: str, callback: Callable[[int, str, str, str], None] | None = None
    ) -> None:
        self._key_watch(key, callback)

    def __setitem__(self, key: str, value: T) -> None:
        self.update(key, value)

    def __delitem__(self, key: str) -> None:
        self.delete(key)

    def _key_watch(
        self, key: str, callback: Callable[[int, str, str, str], None] | None = None
    ) -> None:
        if key not in self._monitoring:
            self._monitoring[key] = []

        if callback in self._monitoring[key]:
            return

        self._monitoring[key].append(callback)
        callbacks = list(self._monitoring[key])
        event_key = KEY_EVENT_FORMAT.format(DB=self._db, KEY=self._build_key(key))

        def callback_wrapper(message: dict[str, bytes]) -> None:
            event_db, event_key, event_type = self._decode_event(message)
            if event_db is None or event_key is None:
                return
            int_event_db = int(event_db)
            str_event_key = str(event_key)
            logger.info(
                "Redis: Handling `%s` for key `%s` (in db %d)...",
                event_type,
                event_key,
                event_db,
            )
            key_value: str = self._key_fetch(str_event_key) or ""
            # NOTE: Reimplement to non-blocking and parallel,
            # instead of sequencial and blocking
            # NOTE: pubsub requires synchronous callbacks
            for callback in callbacks:
                if callback is None:
                    self._on_key_event(
                        int_event_db, str_event_key, event_type, key_value
                    )
                else:
                    callback(int_event_db, str_event_key, event_type, key_value)

        self._pubsub.psubscribe(**{event_key: callback_wrapper})

    def _key_versions(self, key: str) -> int | None:
        try:
            key = self._build_key(key)
            key_type = self._key_type(key)

            if key_type == self.TYPE_LIST:
                return self._redis.llen(key)  # type: ignore
            if key_type == self.TYPE_STRING:
                return 1
            return 0
        except RedisConnectionError:
            logger.error("Redis version: Failed - no connection")
            return self._cache_verisons.get(key)

    def _key_fetch(self, key: str) -> str | None:
        try:
            key = self._build_key(key)
            key_type = self._key_type(key)
            value = None

            if key_type == self.TYPE_LIST:
                value = self._redis.lindex(key, -1)
            if key_type == self.TYPE_STRING:
                value = self._redis.get(key)

            if value is not None:
                return value.decode()  # type: ignore

            return value
        except RedisConnectionError:
            logger.error("Redis fetch: Failed - no connection")
            return self._cache.get(key)

    def _key_update(self, key: str, value: T) -> Any:
        key = self._build_key(key)
        key_type = self._key_type(key)
        value_new = json.dumps(asdict(value))

        try:
            if value_new == json.dumps(self._key_fetch(key)):
                logger.info("Redis update: Identical, skiping change...")
                return value_new
            if key_type in [self.TYPE_LIST, self.TYPE_NONE]:
                self._redis.rpush(key, value_new)
            elif key_type == self.TYPE_STRING:
                value_old = self._redis.get(key)
                self._redis.delete(key)
                self._redis.rpush(key, value_old, value_new)  # type: ignore

            return value_new
        except RedisConnectionError:
            logger.error("Redis update: Failed - no connection")
            return self._cache[key]

    def _key_delete(self, key: str) -> None:
        self._redis.delete(self._build_key(key))

    def _key_type(self, key: str) -> str | None:
        try:
            key_type = self._redis.type(key)
            if key_type is not None:
                return str(key_type.decode())  # type: ignore
            return key_type
        except RedisConnectionError:
            return None

    def _build_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _watcher(self) -> None:
        # This thread seems unecessary, but without it messages aren't updated.
        # Even if we're not receiving any 'pmessage' messages (???)
        while True:
            for message in self._pubsub.listen():
                logger.info(
                    "Redis watcher: Received event: %s - %s",
                    message["channel"],
                    message["data"],
                )
                if message["type"] == "pmessage":
                    event_db, event_key, event_type = self._decode_event(message)
                    if event_db is None or event_key is None:
                        continue
                    int_event_db = int(event_db)
                    str_event_key = str(event_key)
                    key_value = self._key_fetch(str_event_key) or ""

                    self._on_key_event(
                        int_event_db, str_event_key, event_type, key_value
                    )
            time.sleep(1)
        logger.info("Redis watcher is shutting down...")

    def _decode_event(
        self, message: dict[str, bytes]
    ) -> tuple[int | None, str | None, str]:
        channel = message["channel"].decode()

        match = re.match(KEY_EVENT_REGEX, channel)
        if not match:
            return None, None, message["data"].decode()

        event_key = match.group("key")
        event_db = int(match.group("db"))
        event_type = message["data"].decode()
        return event_db, event_key, event_type

    def _on_key_event(self, db: int, key: str, event: str, value: str) -> None:
        if db != self._db:
            return

        if event in [self.EVENT_SET, self.EVENT_RPUSH, self.EVENT_LSET]:
            self._on_key_update(key, value)
        elif event == self.EVENT_DELETE:
            self._on_key_delete(key)
        else:
            logger.info(f"Redis _on_key_event: Ignoring operation: {event}")

    def _on_key_update(self, key: str, value: str) -> None:
        logger.info(f"Redis: Updating local cache: {key}")
        self._cache[key] = value
        self._cache_verisons[key] = self._key_versions(key) or -1

    def _on_key_delete(self, key: str) -> None:
        del self._cache[key]
