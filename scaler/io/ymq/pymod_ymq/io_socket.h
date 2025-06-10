#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <memory>

// First-party
#include "scaler/io/ymq/io_socket.h"

struct PyIOSocket {
    PyObject_HEAD;
    std::shared_ptr<IOSocket> socket;
};

extern "C" {

// static int PyIOSocket_init(PyIOSocket* self, PyObject* args, PyObject* kwds) {
//     return 0;  // todo
// }

static void PyIOSocket_dealloc(PyIOSocket* self) {
    self->socket.~shared_ptr();               // Call the destructor of shared_ptr
    Py_TYPE(self)->tp_free((PyObject*)self);  // Free the PyObject
}

// static PyObject* PyIOSocket_send(PyIOSocket* self, PyObject* args) {}

static PyObject* PyIOSocket_repr(PyIOSocket* self) {
    return PyUnicode_FromFormat("<IOSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyIOSocket_identity_getter(PyIOSocket* self, void* closure) {
    return PyUnicode_FromStringAndSize(self->socket->identity().data(), self->socket->identity().size());
}
}

static PyGetSetDef PyIOSocket_properties[] = {{nullptr, nullptr, nullptr, nullptr, nullptr}};

static PyMethodDef PyIOSocket_methods[] = {{nullptr, nullptr, 0, nullptr}};

static PyType_Slot PyIOSocket_slots[] = {
    {Py_tp_dealloc, (void*)PyIOSocket_dealloc},
    {Py_tp_repr, (void*)PyIOSocket_repr},
    {Py_tp_getset, (void*)PyIOSocket_properties},
    {0, nullptr},
};

static PyType_Spec PyIOSocket_spec = {
    .name      = "ymq.IOSocket",
    .basicsize = sizeof(PyIOSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyIOSocket_slots,
};
