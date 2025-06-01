#define PY_SSIZE_T_CLEAN
#include <Python.h>

// todo: should we have this import?
#include <stddef.h>

// First-party
// #include "io_socket.hpp"

typedef struct {
    PyObject_HEAD;
    void* socket;  // <- the actual socket object
} PyIoSocket;

static int IoSocket_init(PyIoSocket* self, PyObject* args, PyObject* kwds) {
    char* identity = NULL;
    if (!PyArg_ParseTuple(args, "s", &identity))
        return -1;
    return 0;
}

static void IoSocket_dealloc(PyIoSocket* self) {
    Py_TYPE(self)->tp_free(self);
}

static PyObject* IoSocket_repr(PyIoSocket* self) {
    // return PyUnicode_FromFormat("<IoSocket>");
    return PyUnicode_FromString("<IoSocket>");
}

static PyObject* identity_getter(PyIoSocket* self, void* closure) {
    Py_RETURN_NONE;
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

static int ymq_exec(PyObject* module) {
    if (PyType_Ready(&PyIoSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoSocket", (PyObject*)&PyIoSocketType) < 0)
        return -1;

    // Additional initialization code can go here

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

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = PyDoc_STR("YMQ Python bindings"),
    .m_size    = 0,
    .m_methods = ymq_methods,
    .m_slots   = ymq_slots,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}
