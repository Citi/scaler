#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct Awaitable {
    PyObject_HEAD;
    PyObject* future;
};

extern "C" {

static int Awaitable_init(Awaitable* self, PyObject* args, PyObject* kwds) {
    if (!PyArg_ParseTuple(args, "O", &self->future)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to parse arguments for Iterable");
        return -1;
    }

    return 0;
}

static PyObject* Awaitable_await(Awaitable* self) {
    // Easy: coroutines are just iterators and we don't need anything fancy
    // so we can just return the future's iterator!
    return PyObject_GetIter(self->future);
}
}

static PyType_Slot Awaitable_slots[] = {
    {Py_tp_init, (void*)Awaitable_init}, {Py_am_await, (void*)Awaitable_await}, {0, nullptr}};

static PyType_Spec Awaitable_spec {
    .name      = "ymq.Awaitable",
    .basicsize = sizeof(Awaitable),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = Awaitable_slots,
};
