#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// First-party
#include "scaler/io/ymq/bytes.h"

struct PyBytesYmq {
    PyObject_HEAD;
    Bytes bytes;
};

extern "C" {

static int PyBytesYmq_init(PyBytesYmq* self, PyObject* args, PyObject* kwds) {
    PyObject* bytes        = nullptr;
    const char* keywords[] = {"bytes", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", (char**)keywords, &bytes)) {
        return -1;  // Error parsing arguments
    }

    if (!bytes) {
        // If no bytes were provided, initialize with an empty Bytes object
        self->bytes = Bytes::empty();
        return 0;
    }

    if (!PyBytes_Check(bytes)) {
        bytes = PyObject_Bytes(bytes);

        if (!bytes) {
            PyErr_SetString(PyExc_TypeError, "Expected bytes or bytes-like object");
            return -1;
        }
    }

    char* data;
    Py_ssize_t len;

    if (PyBytes_AsStringAndSize(bytes, &data, &len) < 0) {
        PyErr_SetString(PyExc_TypeError, "Failed to get bytes data");
        return -1;
    }

    // copy the data into the Bytes object
    // it might be possible to make this zero-copy in the future
    self->bytes = Bytes::copy((uint8_t*)data, len);

    return 0;
}

static void PyBytesYmq_dealloc(PyBytesYmq* self) {
    self->bytes.~Bytes();  // Call the destructor of Bytes
    Py_TYPE(self)->tp_free(self);
}

static PyObject* PyBytesYmq_repr(PyBytesYmq* self) {
    if (self->bytes.is_empty()) {
        return PyUnicode_FromString("<Bytes: empty>");
    } else {
        return PyUnicode_FromFormat("<Bytes: %db>", self->bytes.len());
    }
}

static PyObject* PyBytesYmq_data_getter(PyBytesYmq* self) {
    return PyBytes_FromStringAndSize((const char*)self->bytes.data(), self->bytes.len());
}

static PyObject* PyBytesYmq_len_getter(PyBytesYmq* self) {
    return PyLong_FromSize_t(self->bytes.len());
}

static int PyBytesYmq_getbuffer(PyBytesYmq* self, Py_buffer* view, int flags) {
    return PyBuffer_FillInfo(view, (PyObject*)self, (void*)self->bytes.data(), self->bytes.len(), true, flags);
}
}

static PyGetSetDef PyBytesYmq_properties[] = {
    {"data", (getter)PyBytesYmq_data_getter, nullptr, PyDoc_STR("Data of the Bytes object"), nullptr},
    {"len", (getter)PyBytesYmq_len_getter, nullptr, PyDoc_STR("Length of the Bytes object"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}  // Sentinel
};

static PyBufferProcs PyBytesYmqBufferProcs = {
    .bf_getbuffer     = (getbufferproc)PyBytesYmq_getbuffer,
    .bf_releasebuffer = (releasebufferproc) nullptr,
};

// clang-format off
static PyTypeObject PyBytesYmqType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Bytes",
    .tp_basicsize = sizeof(PyBytesYmq),
    .tp_itemsize  = 0,
    .tp_dealloc   = (destructor)PyBytesYmq_dealloc,
    .tp_repr      = (reprfunc)PyBytesYmq_repr,
    .tp_as_buffer = &PyBytesYmqBufferProcs,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = PyDoc_STR("Bytes"),
    .tp_getset    = PyBytesYmq_properties,
    .tp_init      = (initproc)PyBytesYmq_init,
    .tp_new       = PyType_GenericNew,
};
// clang-format on
