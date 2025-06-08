#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// First-party
#include "scaler/io/ymq/pymod_ymq/bytes.h"

struct PyMessage {
    PyObject_HEAD;
    PyBytesYmq* address;  // Address of the message
    PyBytesYmq* payload;  // Payload of the message
};

extern "C" {

static int PyMessage_init(PyMessage* self, PyObject* args, PyObject* kwds) {
    PyObject *address = nullptr, *payload = nullptr;
    const char* keywords[] = {"address", "payload", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", (char**)keywords, &address, &payload)) {
        PyErr_SetString(PyExc_TypeError, "Expected two Bytes objects: address and payload");
        return -1;
    }

    // check if the address and payload are of type PyBytesYmq
    if (!PyObject_TypeCheck(address, &PyBytesYmqType)) {
        PyObject* args = PyTuple_Pack(1, address);
        address        = PyObject_CallObject((PyObject*)&PyBytesYmqType, args);
        Py_DECREF(args);

        if (!address) {
            return -1;
        }
    }

    if (!PyObject_TypeCheck(payload, &PyBytesYmqType)) {
        PyObject* args = PyTuple_Pack(1, payload);
        payload        = PyObject_CallObject((PyObject*)&PyBytesYmqType, args);
        Py_DECREF(args);

        if (!payload) {
            return -1;
        }
    }

    self->address = (PyBytesYmq*)address;
    self->payload = (PyBytesYmq*)payload;

    return 0;  // todo
}

static void PyMessage_dealloc(PyMessage* self) {
    Py_XDECREF(self->address);
    Py_XDECREF(self->payload);
    Py_TYPE(self)->tp_free(self);
}

static PyObject* PyMessage_repr(PyMessage* self) {
    return PyUnicode_FromFormat("<Message address=%R payload=%R>", self->address, self->payload);
}
}

static PyMemberDef PyMessage_members[] = {{nullptr}};

// clang-format off
static PyTypeObject PyMessageType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Message",
    .tp_basicsize = sizeof(PyMessage),
    .tp_itemsize  = 0,
    .tp_dealloc   = (destructor)PyMessage_dealloc,
    .tp_repr      = (reprfunc)PyMessage_repr,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = PyDoc_STR("Message"),
    .tp_members   = PyMessage_members,
    .tp_init      = (initproc)PyMessage_init,
    .tp_new       = PyType_GenericNew,
};
// clang-format on
