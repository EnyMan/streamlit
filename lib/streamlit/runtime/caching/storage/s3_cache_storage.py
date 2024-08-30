# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022-2024)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import math
import os
from io import BytesIO
from typing import Final

import boto3
from botocore.exceptions import ClientError

from streamlit.logger import get_logger
from streamlit.runtime.caching.storage.cache_storage_protocol import (
    CacheStorage,
    CacheStorageContext,
    CacheStorageError,
    CacheStorageKeyNotFoundError,
    CacheStorageManager,
)
from streamlit.runtime.caching.storage.in_memory_cache_storage_wrapper import (
    InMemoryCacheStorageWrapper,
)

_LOGGER: Final = get_logger(__name__)

# Streamlit directory where persisted @st.cache_data objects live.
# (This is the same directory that @st.cache persisted objects live.
# But @st.cache_data uses a different extension, so they don't overlap.)
_CACHE_BUCKET_NAME: Final = os.getenv("STREAMLIT_S3_BUCKET", "streamlit-cache")

# The extension for our persisted @st.cache_data objects.
# (`@st.cache_data` was originally called `@st.memo`)
_CACHED_FILE_EXTENSION: Final = "memo"


class S3CacheStorageManager(CacheStorageManager):
    def create(self, context: CacheStorageContext) -> CacheStorage:
        """Creates a new cache storage instance wrapped with in-memory cache layer"""
        persist_storage = S3CacheStorage(context)
        return InMemoryCacheStorageWrapper(
            persist_storage=persist_storage, context=context
        )

    def clear_all(self) -> None:
        s3_resource = boto3.resource("s3")
        s3_bucket = s3_resource.Bucket(_CACHE_BUCKET_NAME)
        # s3_bucket.objects.delete() does this work?
        for key in s3_bucket.objects.all():
            key.delete()

    def check_context(self, context: CacheStorageContext) -> None:
        if (
            context.persist == "disk"
            and context.ttl_seconds is not None
            and not math.isinf(context.ttl_seconds)
        ):
            _LOGGER.warning(
                f"The cached function '{context.function_display_name}' has a TTL "
                "that will be ignored. Persistent cached functions currently don't "
                "support TTL."
            )


class S3CacheStorage(CacheStorage):
    """Cache storage that persists data to disk
    This is the default cache persistence layer for `@st.cache_data`
    """

    def __init__(self, context: CacheStorageContext):
        self.function_key = context.function_key
        self.persist = context.persist
        self._ttl_seconds = context.ttl_seconds
        self._max_entries = context.max_entries
        self.s3_resource = boto3.resource("s3")

    @property
    def ttl_seconds(self) -> float:
        return math.inf

    @property
    def max_entries(self) -> float:
        return math.inf

    def get(self, key: str) -> bytes:
        """
        Returns the stored value for the key if persisted,
        raise CacheStorageKeyNotFoundError if not found, or not configured
        with persist="disk"
        """
        if self.persist == "disk":
            path = self._get_cache_file_path(key)
            try:
                s3_object = self.s3_resource.Object(_CACHE_BUCKET_NAME, path)
                with BytesIO() as data:
                    s3_object.download_fileobj(data)
                    return bytes(data.read())
            except ClientError:
                raise CacheStorageKeyNotFoundError("Key not found in disk cache")
            except Exception as ex:
                _LOGGER.error(ex)
                raise CacheStorageError("Unable to read from cache") from ex
        else:
            raise CacheStorageKeyNotFoundError(
                f"Local disk cache storage is disabled (persist={self.persist})"
            )

    def set(self, key: str, value: bytes) -> None:
        """Sets the value for a given key"""
        if self.persist == "disk":
            path = self._get_cache_file_path(key)
            try:
                s3_object = self.s3_resource.Object(_CACHE_BUCKET_NAME, path)
                with BytesIO(value) as data:
                    s3_object.upload_fileobj(data)
            except ClientError as e:
                _LOGGER.debug(e)
                raise CacheStorageError("Unable to write to cache") from e
            except Exception as ex:
                _LOGGER.error(ex)
                raise CacheStorageError("Unable to write to cache") from ex

    def delete(self, key: str) -> None:
        """Delete a cache file from disk. If the file does not exist on disk,
        return silently. If another exception occurs, log it. Does not throw.
        """
        if self.persist == "disk":
            path = self._get_cache_file_path(key)
            try:
                s3_object = self.s3_resource.Object(_CACHE_BUCKET_NAME, path)
                s3_object.delete()
            except ClientError:
                # The file is already removed.
                pass
            except Exception as ex:
                _LOGGER.exception(
                    "Unable to remove a file from the disk cache", exc_info=ex
                )

    def clear(self) -> None:
        """Delete all keys for the current storage"""
        cache_bucket = _CACHE_BUCKET_NAME
        s3_bucket = self.s3_resource.Bucket(cache_bucket)
        for key in s3_bucket.objects.all():
            if self._is_cache_file(key.key):
                key.delete()

    def close(self) -> None:
        """Dummy implementation of close, we don't need to actually "close" anything"""

    def _get_cache_file_path(self, value_key: str) -> str:
        """Return the path of the disk cache file for the given value."""
        return f"{self.function_key}-{value_key}.{_CACHED_FILE_EXTENSION}"

    def _is_cache_file(self, fname: str) -> bool:
        """Return true if the given file name is a cache file for this storage."""
        return fname.startswith(f"{self.function_key}-") and fname.endswith(
            f".{_CACHED_FILE_EXTENSION}"
        )
