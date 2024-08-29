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

import pickle

from pyredis import RedisConnection

from streamlit.runtime.session_manager import SessionInfo, SessionStorage


class RedisSessionStorage(SessionStorage):
    """A SessionStorage that stores sessions in redis.
    """
    def __init__(
        self,
    ) -> None:
        """Instantiate a new RedisSessionStorage.
        """
        self._cache = RedisConnection()  # some args go here

    def get(self, session_id: str) -> SessionInfo | None:
        return pickle.loads(self._cache.get(session_id)["data"])

    def save(self, session_info: SessionInfo) -> None:
        self._cache.set(session_info.session.id, {"data": pickle.dumps(session_info)})

    def delete(self, session_id: str) -> None:
        self._cache.R.delete(session_id)

    def list(self) -> list[SessionInfo]:
        return [pickle.loads(value) for key, value in self._cache.get_keys("*")]
