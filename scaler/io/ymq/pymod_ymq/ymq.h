#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <string>
#include <utility>

// First-Party
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/io_context.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/message.h"

struct YmqState {
    PyObject* enumModule;       // Reference to the enum module
    PyObject* socketTypesEnum;  // Reference to the SocketTypes enum
};

extern "C" {

static int ymq_init(PyObject* module) {
    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

    state->enumModule = PyImport_ImportModule("enum");

    if (!state->enumModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to import enum module");
        return -1;
    }

    return 0;
}

static void ymq_free(YmqState* state) {
    Py_XDECREF(state->enumModule);
    Py_XDECREF(state->socketTypesEnum);
    state->enumModule = nullptr;
}

static int ymq_createIntEnum(PyObject* module, std::string enumName, std::vector<std::pair<std::string, int>> entries) {
    // create a python dictionary to hold the entries
    auto enumDict = PyDict_New();
    if (!enumDict) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create enum dictionary");
        return -1;
    }

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        PyObject* value = PyLong_FromLong(entry.second);
        if (!value) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create enum value");
            Py_DECREF(enumDict);
            return -1;
        }

        if (PyDict_SetItemString(enumDict, entry.first.c_str(), value) < 0) {
            Py_DECREF(value);
            Py_DECREF(enumDict);
            PyErr_SetString(PyExc_RuntimeError, "Failed to set item in enum dictionary");
            return -1;
        }
        Py_DECREF(value);
    }

    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        Py_DECREF(enumDict);
        return -1;
    }

    // create our class by calling enum.IntEnum(enumName, enumDict)
    auto socketTypesEnum = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!socketTypesEnum) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IntEnum class");
        return -1;
    }

    state->socketTypesEnum = socketTypesEnum;

    // add the class to the module
    // this increments the reference count of enumClass
    if (PyModule_AddObjectRef(module, enumName.c_str(), socketTypesEnum) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add IntEnum class to module");
        Py_DECREF(socketTypesEnum);
        return -1;
    }

    return 0;
}

static int ymq_createSocketTypesEnum(PyObject* module) {
    std::vector<std::pair<std::string, int>> socketTypes = {
        {"Binder", (int)SocketTypes::Binder},
        {"Sub", (int)SocketTypes::Sub},
        {"Pub", (int)SocketTypes::Pub},
        {"Dealer", (int)SocketTypes::Dealer},
        {"Router", (int)SocketTypes::Router},
        {"Pair", (int)SocketTypes::Pair}};

    if (ymq_createIntEnum(module, "SocketTypes", socketTypes) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create SocketTypes enum");
        return -1;
    }

    return 0;
}

static int ymq_exec(PyObject* module) {
    if (ymq_init(module) < 0)
        return -1;

    if (ymq_createSocketTypesEnum(module) < 0)
        return -1;

    if (PyType_Ready(&PyBytesYmqType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Bytes", (PyObject*)&PyBytesYmqType) < 0)
        return -1;

    if (PyType_Ready(&PyMessageType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Message", (PyObject*)&PyMessageType) < 0)
        return -1;

    if (PyType_Ready(&PyIOSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IOSocket", (PyObject*)&PyIOSocketType) < 0)
        return -1;

    if (PyType_Ready(&PyIOContextType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IOContext", (PyObject*)&PyIOContextType) < 0)
        return -1;

    return 0;
}
}

static PyModuleDef_Slot ymq_slots[] = {{Py_mod_exec, (void*)ymq_exec}, {0, NULL}};

static PyModuleDef ymq_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "ymq",
    .m_doc   = PyDoc_STR("YMQ Python bindings"),
    .m_size  = sizeof(YmqState),
    .m_slots = ymq_slots,
    .m_free  = (freefunc)ymq_free,
};

PyMODINIT_FUNC PyInit_ymq(void);
