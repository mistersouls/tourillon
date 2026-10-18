# Copyright 2026 Tourillon Contributors
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

import functools
import importlib
import inspect
import pkgutil
from collections.abc import Callable


def parse_bind(endpoint: str) -> tuple[str, int]:
    host, sep, port_text = endpoint.rpartition(":")
    if sep == "" or not host or not port_text.isdigit():
        raise ValueError(f"invalid endpoint format: {endpoint}")
    return host, int(port_text)


def scan(package: str):
    """
    Decorator that triggers a component scan when the decorated callable
    is invoked. Works with both regular functions and coroutine functions.
    """

    def _do_scan() -> None:
        py_package = importlib.import_module(package)
        for module_info in pkgutil.iter_modules(py_package.__path__):
            module_name = f"{package}.{module_info.name}"
            importlib.import_module(module_name)

    def decorator(func: Callable):
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                _do_scan()
                return await func(*args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _do_scan()
            return func(*args, **kwargs)

        return wrapper

    return decorator
