#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <memory>

// First-party
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

struct PyIOContext {
    PyObject_HEAD;
    std::shared_ptr<IOContext> ioContext;
};

extern "C" {

static int PyIOContext_init(PyIOContext* self, PyObject* args, PyObject* kwds) {
    self->ioContext = std::make_shared<IOContext>();
    return 0;
}

static void PyIOContext_dealloc(PyIOContext* self) {
    // Python knows nothing about C++, so we need to manually call destructors
    self->ioContext.~shared_ptr();            // Call the destructor of shared_ptr
    Py_TYPE(self)->tp_free((PyObject*)self);  // Free the PyObject
}

static PyObject* PyIOContext_repr(PyIOContext* self) {
    return PyUnicode_FromFormat("<IOContext at %p>", (void*)self->ioContext.get());
}

// todo: how to parse keyword arguments?
// https://docs.python.org/3/c-api/structures.html#c.METH_METHOD
// https://docs.python.org/3.10/c-api/call.html#vectorcall
// https://peps.python.org/pep-0590/
static PyObject* PyIOContext_createIOSocket(
    PyIOContext* self, PyTypeObject* clazz, PyObject* const* args, Py_ssize_t nargs, PyObject* kwnames) {
    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "createIOSocket() requires exactly two arguments: identity and socket_type");
        return nullptr;
    }

    PyObject *pyIdentity = args[0], *pySocketType = args[1];

    if (!PyUnicode_Check(pyIdentity)) {
        PyErr_SetString(PyExc_TypeError, "Expected identity to be a string");
        return nullptr;
    }

    // get the module state from the class
    YmqState* state = (YmqState*)PyType_GetModuleState(clazz);

    if (!state) {
        // PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    if (!PyObject_IsInstance(pySocketType, state->socketTypesEnum)) {
        PyErr_SetString(PyExc_TypeError, "Expected socket_type to be an instance of SocketTypes");
        return nullptr;
    }

    Py_ssize_t identitySize;
    const char* identityCStr = PyUnicode_AsUTF8AndSize(pyIdentity, &identitySize);

    if (!identityCStr) {
        PyErr_SetString(PyExc_TypeError, "Failed to convert identity to string");
        return nullptr;
    }

    PyObject* value = PyObject_GetAttrString(pySocketType, "value");

    if (!value) {
        PyErr_SetString(PyExc_TypeError, "Failed to get value from socket_type");
        return nullptr;
    }

    if (!PyLong_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "Expected socket_type to be an integer");
        Py_DECREF(value);
        return nullptr;
    }

    long socketTypeValue = PyLong_AsLong(value);

    if (socketTypeValue < 0 && PyErr_Occurred()) {
        PyErr_SetString(PyExc_TypeError, "Failed to convert socket_type to integer");
        Py_DECREF(value);
        return nullptr;
    }

    Py_DECREF(value);

    Identity identity(identityCStr, identitySize);
    SocketTypes socketType = static_cast<SocketTypes>(socketTypeValue);

    PyIOSocket* ioSocket = PyObject_New(PyIOSocket, (PyTypeObject*)state->PyIOSocketType);

    if (!ioSocket) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocket instance");
        return nullptr;
    }

    printf("1\n");
    ioSocket->socket = self->ioContext->createIOSocket(identity, socketType);
    printf("2\n");

    return (PyObject*)ioSocket;
}
}

static PyMethodDef PyIOContext_methods[] = {
    {"createIOSocket",
     (PyCFunction)PyIOContext_createIOSocket,
     METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
     PyDoc_STR("Create a new IOSocket")},
    {nullptr, nullptr, 0, nullptr}};

static PyType_Slot PyIOContext_slots[] = {
    {Py_tp_init, (void*)PyIOContext_init},
    {Py_tp_dealloc, (void*)PyIOContext_dealloc},
    {Py_tp_repr, (void*)PyIOContext_repr},
    {Py_tp_methods, (void*)PyIOContext_methods},
    {0, nullptr},
};

static PyType_Spec PyIOContext_spec = {
    .name      = "ymq.IOContext",
    .basicsize = sizeof(PyIOContext),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyIOContext_slots,
};
