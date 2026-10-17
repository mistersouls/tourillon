/*
 * Copyright 2026 Tourillon Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

#define PATH_CAPACITY 32768

#ifdef TOURCTL_LAUNCHER
#define PRODUCT_MODULE_WIDE L"tourctl"
#define PRODUCT_MODULE_BYTES "tourctl"
#else
#define PRODUCT_MODULE_WIDE L"tourillon"
#define PRODUCT_MODULE_BYTES "tourillon"
#endif

static PyStatus initialize_config(PyConfig *config) {
    PyPreConfig preconfig;
    PyPreConfig_InitIsolatedConfig(&preconfig);
    preconfig.utf8_mode = 1;

    PyConfig_InitIsolatedConfig(config);
    config->parse_argv = 0;
    config->site_import = 0;
    config->user_site_directory = 0;
    config->write_bytecode = 0;
    config->module_search_paths_set = 1;

    PyStatus status = Py_PreInitialize(&preconfig);
    if (PyStatus_Exception(status)) {
        return status;
    }
    return PyConfig_SetString(config, &config->stdio_encoding, L"utf-8");
}

#ifdef _WIN32
#include <windows.h>

static int executable_directory(wchar_t *directory, size_t capacity) {
    DWORD length = GetModuleFileNameW(NULL, directory, (DWORD)capacity);
    if (length == 0 || length >= capacity) {
        return 0;
    }
    wchar_t *separator = wcsrchr(directory, L'\\');
    if (separator == NULL) {
        separator = wcsrchr(directory, L'/');
    }
    if (separator == NULL) {
        return 0;
    }
    *separator = L'\0';
    return 1;
}

static PyStatus append_path(PyConfig *config, const wchar_t *path) {
    if (GetFileAttributesW(path) == INVALID_FILE_ATTRIBUTES) {
        return PyStatus_Ok();
    }
    return PyWideStringList_Append(&config->module_search_paths, path);
}

static PyStatus append_runtime_paths(PyConfig *config, const wchar_t *root) {
    wchar_t path[PATH_CAPACITY];
    if (swprintf_s(path, PATH_CAPACITY, L"%ls\\python%u%u.zip", root,
                   PY_MAJOR_VERSION, PY_MINOR_VERSION) < 0) {
        return PyStatus_Error("Unable to construct the standard library path.");
    }
    PyStatus status = append_path(config, path);
    if (PyStatus_Exception(status)) {
        return status;
    }
    if (swprintf_s(path, PATH_CAPACITY, L"%ls\\DLLs", root) < 0) {
        return PyStatus_Error("Unable to construct the native module path.");
    }
    status = append_path(config, path);
    if (PyStatus_Exception(status)) {
        return status;
    }
    return append_path(config, root);
}

int wmain(int argc, wchar_t *argv[]) {
    wchar_t root[PATH_CAPACITY];
    if (!executable_directory(root, PATH_CAPACITY)) {
        fwprintf(stderr, L"Unable to determine the launcher directory.\n");
        return 1;
    }

    PyConfig config;
    PyStatus status = initialize_config(&config);
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetString(&config, &config.home, root);
    }
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetString(&config, &config.program_name, argv[0]);
    }
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetArgv(&config, argc, argv);
    }
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetString(&config, &config.run_module, PRODUCT_MODULE_WIDE);
    }
    if (!PyStatus_Exception(status)) {
        status = append_runtime_paths(&config, root);
    }
    if (PyStatus_Exception(status)) {
        PyConfig_Clear(&config);
        Py_ExitStatusException(status);
    }

    status = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);
    if (PyStatus_Exception(status)) {
        Py_ExitStatusException(status);
    }
    return Py_RunMain();
}

#else
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

static int executable_directory(char *directory, size_t capacity) {
#ifdef __APPLE__
    uint32_t length = (uint32_t)capacity;
    if (_NSGetExecutablePath(directory, &length) != 0) {
        return 0;
    }
    char resolved[PATH_MAX];
    if (realpath(directory, resolved) == NULL) {
        return 0;
    }
    if (snprintf(directory, capacity, "%s", resolved) >= (int)capacity) {
        return 0;
    }
#else
    ssize_t length = readlink("/proc/self/exe", directory, capacity - 1);
    if (length <= 0 || (size_t)length >= capacity - 1) {
        return 0;
    }
    directory[length] = '\0';
#endif
    char *separator = strrchr(directory, '/');
    if (separator == NULL) {
        return 0;
    }
    *separator = '\0';
    return 1;
}

static PyStatus append_path(PyConfig *config, const char *path) {
    struct stat info;
    if (stat(path, &info) != 0) {
        return PyStatus_Ok();
    }
    wchar_t *wide_path = Py_DecodeLocale(path, NULL);
    if (wide_path == NULL) {
        return PyStatus_Error("Unable to decode a Python module path.");
    }
    PyStatus status = PyWideStringList_Append(&config->module_search_paths, wide_path);
    PyMem_RawFree(wide_path);
    return status;
}

static PyStatus append_runtime_paths(PyConfig *config, const char *root) {
    char path[PATH_CAPACITY];
    if (snprintf(path, sizeof(path), "%s/python%u%u.zip", root,
                 PY_MAJOR_VERSION, PY_MINOR_VERSION) >= (int)sizeof(path)) {
        return PyStatus_Error("Unable to construct the standard library path.");
    }
    PyStatus status = append_path(config, path);
    if (PyStatus_Exception(status)) {
        return status;
    }
    if (snprintf(path, sizeof(path), "%s/lib/python%u.%u/lib-dynload", root,
                 PY_MAJOR_VERSION, PY_MINOR_VERSION) >= (int)sizeof(path)) {
        return PyStatus_Error("Unable to construct the native module path.");
    }
    status = append_path(config, path);
    if (PyStatus_Exception(status)) {
        return status;
    }
    return append_path(config, root);
}

int main(int argc, char *argv[]) {
    char root[PATH_CAPACITY];
    if (!executable_directory(root, sizeof(root))) {
        fputs("Unable to determine the launcher directory.\n", stderr);
        return 1;
    }

    PyConfig config;
    PyStatus status = initialize_config(&config);
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetBytesString(&config, &config.home, root);
    }
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetBytesArgv(&config, argc, argv);
    }
    if (!PyStatus_Exception(status)) {
        status = PyConfig_SetBytesString(&config, &config.run_module, PRODUCT_MODULE_BYTES);
    }
    if (!PyStatus_Exception(status)) {
        status = append_runtime_paths(&config, root);
    }
    if (PyStatus_Exception(status)) {
        PyConfig_Clear(&config);
        Py_ExitStatusException(status);
    }

    status = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);
    if (PyStatus_Exception(status)) {
        Py_ExitStatusException(status);
    }
    return Py_RunMain();
}
#endif
