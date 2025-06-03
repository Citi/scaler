#define PY_SSIZE_T_CLEAN
#include <Python.h>

// C
#include <stddef.h>

// C++
#include <utility>
#include <vector>

// First-party
#include "io_socket.hpp"

struct YmqState {
    PyObject* enumModule;  // Reference to the enum module
};

typedef struct {
    PyObject_HEAD;
    IOSocket* socket;  // <- the actual socket object
} PyIoSocket;

static int IoSocket_init(PyIoSocket* self, PyObject* args, PyObject* kwds) {
    char* identity = NULL;
    if (!PyArg_ParseTuple(args, "s", &identity))
        return -1;
    self->socket = nullptr;  // TODO!
    return 0;
}

static void IoSocket_dealloc(PyIoSocket* self) {
    self->socket->~IOSocket();
    self->socket = nullptr;

    Py_TYPE(self)->tp_free(self);
}

static PyObject* IoSocket_repr(PyIoSocket* self) {
    // return PyUnicode_FromFormat("<IoSocket>");
    return PyUnicode_FromString("<IoSocket>");
}

static PyObject* identity_getter(PyIoSocket* self, void* closure) {
    return PyUnicode_FromStringAndSize(self->socket->identity.data(), self->socket->identity.size());
}

static PyGetSetDef IoSocket_properties[] = {
    {"identity", (getter)identity_getter, nullptr, PyDoc_STR("Identity of the IoSocket"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}  // Sentinel
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"
// clang-format off
// this ordering is canonical as per the Python documentation
static PyTypeObject PyIoSocketType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.IoSocket",
    .tp_doc       = PyDoc_STR("IoSocket"),
    .tp_basicsize = sizeof(PyIoSocket),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)IoSocket_init,
    .tp_repr      = (reprfunc)IoSocket_repr,
    .tp_dealloc   = (destructor)IoSocket_dealloc,
    .tp_getset    = IoSocket_properties,
};
// clang-format on
#pragma clang diagnostic pop

static int createIntEnum(PyObject* module, std::string enumName, std::vector<std::pair<std::string, int>> entries) {
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
    auto enumClass = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!enumClass) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IntEnum class");
        return -1;
    }

    // add the class to the module
    // this increments the reference count of enumClass
    if (PyModule_AddObjectRef(module, enumName.c_str(), enumClass) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add IntEnum class to module");
        Py_DECREF(enumClass);
        return -1;
    }
    Py_DECREF(enumClass);

    return 0;
}

static int createSocketTypesEnum(PyObject* module) {
    std::vector<std::pair<std::string, int>> socketTypes = {
        {
            "Binder",
            (int)SocketTypes::Binder,
        },
        {"Sub", (int)SocketTypes::Sub},
        {"Pub", (int)SocketTypes::Pub},
        {"Dealer", (int)SocketTypes::Dealer},
        {"Router", (int)SocketTypes::Router},
        {"Pair", (int)SocketTypes::Pair}};

    if (createIntEnum(module, "SocketTypes", socketTypes) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create SocketTypes enum");
        return -1;
    }

    return 0;
}

static int ymq_exec(PyObject* module) {
    if (PyType_Ready(&PyIoSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoSocket", (PyObject*)&PyIoSocketType) < 0)
        return -1;

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

    if (createSocketTypesEnum(module) < 0)
        return -1;

    return 0;
}

static PyMethodDef ymq_methods[] = {
    {NULL, NULL, 0, NULL}  // Sentinel
};

static PyModuleDef_Slot ymq_slots[] = {
    {Py_mod_exec, (void*)ymq_exec},
    // only supported in Python 3.12+
    // {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED},
    {0, NULL}  // Sentinel
};

void ymq_free(YmqState* state) {
    Py_DECREF(state->enumModule);
    state->enumModule = nullptr;
}

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = PyDoc_STR("YMQ Python bindings"),
    .m_size    = sizeof(YmqState),
    .m_methods = ymq_methods,
    .m_slots   = ymq_slots,
    .m_free    = (freefunc)ymq_free,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}
