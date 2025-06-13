#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// First-party
#include "scaler/io/ymq/pymod_ymq/ymq.h"

// implements a Python awaitable in C/C++
// https://stackoverflow.com/a/51115745

// BEGIN AWAITABLE

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
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* module = PyType_GetModule(Py_TYPE(self));
    if (!module) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return nullptr;
    }

    auto state = (YmqState*)PyModule_GetState(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    return PyObject_CallFunction(state->IterableType, "O", self->future);
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

// END AWAITABLE
// BEGIN ITERABLE

enum IteratorState {
    Polling,
    Done,
};

struct Iterable {
    PyObject_HEAD;
    PyObject* future;
    IteratorState state;
};

extern "C" {

static int Iterable_init(Iterable* self, PyObject* args, PyObject* kwds) {
    if (!PyArg_ParseTuple(args, "O", &self->future)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to parse arguments for Iterable");
        return -1;
    }

    self->state = Polling;

    return 0;
}

static PyObject* Iterable_iternext(Iterable* self) {
    // return Py_TYPE(self->future)->tp_iternext(self->future);

    PyObject* iter = PyObject_GetIter(self->future);

    if (!iter) {
        Py_DECREF(iter);
        PyErr_SetString(PyExc_RuntimeError, "Failed to call iter() on future");
        Py_RETURN_NONE;
    }

    PyObject* next = PyIter_Next(iter);
    Py_DECREF(iter);

    return next;
}
}

static PyType_Slot Iterable_slots[] = {
    {Py_tp_init, (void*)Iterable_init},
    {Py_tp_iter, (void*)PyObject_SelfIter},
    {Py_tp_iternext, (void*)Iterable_iternext},
    {0, nullptr}};

static PyType_Spec Iterable_spec {
    .name      = "ymq.Iterable",
    .basicsize = sizeof(Iterable),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = Iterable_slots,
};

// END ITERABLE
