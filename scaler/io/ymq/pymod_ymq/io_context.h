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

// todo: how to parse the arguments?
// https://docs.python.org/3/c-api/structures.html#c.METH_METHOD
// https://docs.python.org/3.10/c-api/call.html#vectorcall
// https://peps.python.org/pep-0590/
static PyObject* PyIOContext_createIOSocket(
    PyIOContext* self, PyTypeObject* clazz, PyObject* const* args, Py_ssize_t nargs, PyObject* kwnames) {
    PyObject *pyIdentity = nullptr, *pySocketType = nullptr;

    // Parse the arguments
    if (!PyArg_ParseTupleAndKeywords(args, nargs, kwnames, "OO:createIOSocket", &pyIdentity, &pySocketType)) {
        PyErr_SetString(PyExc_TypeError, "Expected two arguments: identity (str) and socket_type (SocketTypes)");
        return nullptr;
    }

    if (!PyUnicode_Check(pyIdentity)) {
        PyErr_SetString(PyExc_TypeError, "Expected identity to be a string");
        return nullptr;
    }

    // get the module state from the class
    YmqState* state = (YmqState*)PyType_GetModuleState(clazz);

    if (!PyObject_IsInstance(pySocketType, state->socketTypesEnum)) {
        PyErr_SetString(PyExc_TypeError, "Expected socket_type to be an instance of SocketTypes");
        return nullptr;
    }
    // Convert Python string to C++ std::string
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

    PyIOSocket* ioSocket = PyObject_NEW(PyIOSocket, &PyIOSocketType);
    ioSocket->socket     = self->ioContext->createIOSocket(identity, socketType);

    return (PyObject*)ioSocket;
}
}

static PyMethodDef PyIOContext_methods[] = {
    {"createIOSocket",
     (PyCFunction)PyIOContext_createIOSocket,
     METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
     PyDoc_STR("Create a new IOSocket")},
    {nullptr, nullptr, 0, nullptr}};

// clang-format off
static PyTypeObject PyIOContextType = {
    .ob_base      = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.IOContext",
    .tp_basicsize = sizeof(PyIOContext),
    .tp_itemsize  = 0,
    .tp_dealloc   = (destructor)PyIOContext_dealloc,
    .tp_repr      = (reprfunc)PyIOContext_repr,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = PyDoc_STR("IOContext"),
    .tp_methods   = PyIOContext_methods,
    .tp_init      = (initproc)PyIOContext_init,
    .tp_new       = PyType_GenericNew,
};
// clang-format on
