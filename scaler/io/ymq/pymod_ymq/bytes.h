// allows us to define PyTypeObjects in the canonical way without warnings
#include "scaler/io/ymq/bytes.h"
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct PyBytesYmq {
    PyObject_HEAD;
    Bytes bytes;
    int shares;  // Reference count for buffer sharing
};

extern "C" {

static int PyBytesYmq_init(PyBytesYmq* self, PyObject* args, PyObject* kwds) {    
    return 0;  // todo
}

static void PyBytesYmq_dealloc(PyBytesYmq* self) {
    printf("PyBytesYmq_dealloc called, shares: %d\n", self->shares);

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
    self->shares++;

    return PyBuffer_FillInfo(view, (PyObject*)self, (void*)self->bytes.data(), self->bytes.len(), true, flags);
}

static void PyBytesYmq_releasebuffer(PyBytesYmq* self, Py_buffer* view) {
    self->shares--;

    if (self->shares == 0) {
        // If no more references, we can free the data
        self->bytes.~Bytes();  // Call the destructor of Bytes
    }
}
}

static PyGetSetDef PyBytesYmq_properties[] = {
    {"data", (getter)PyBytesYmq_data_getter, nullptr, PyDoc_STR("Data of the Bytes object"), nullptr},
    {"len", (getter)PyBytesYmq_len_getter, nullptr, PyDoc_STR("Length of the Bytes object"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}  // Sentinel
};

static PyBufferProcs PyBytesYmqBufferProcs = {
    .bf_getbuffer     = (getbufferproc)PyBytesYmq_getbuffer,
    .bf_releasebuffer = (releasebufferproc)PyBytesYmq_releasebuffer,
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
