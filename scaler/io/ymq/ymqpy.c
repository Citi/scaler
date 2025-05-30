#define PY_SSIZE_T_CLEAN
#include <Python.h>

// todo: should we have these imports?
#include <stddef.h>
#include <structmember.h>

typedef struct {
    PyObject_HEAD;
    char* identity;
} IoSocket;

static int IoSocket_init(IoSocket* self, PyObject* args, PyObject* kwds) {
    char* identity = NULL;
    if (!PyArg_ParseTuple(args, "s", &identity))
        return -1;
    self->identity = strdup(identity);
    return 0;
}

static void IoSocket_dealloc(IoSocket* self) {
    free(self->identity);
    Py_TYPE(self)->tp_free(self);
}

PyObject* IoSocket_repr(IoSocket* self) {
    return PyUnicode_FromFormat("<IoSocket identity=\"%s\">", self->identity);
}

static PyMemberDef IoSocket_members[] = {
    {"identity", T_STRING, offsetof(IoSocket, identity), READONLY, PyDoc_STR("Identity of the IoSocket")},
    {NULL}  // Sentinel
};

static PyTypeObject IoSocketType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "ymq.IoSocket",
    .tp_doc                                           = PyDoc_STR("IoSocket"),
    .tp_basicsize                                     = sizeof(IoSocket),
    .tp_itemsize                                      = 0,
    .tp_flags                                         = Py_TPFLAGS_DEFAULT,
    .tp_new                                           = PyType_GenericNew,
    .tp_init                                          = (initproc)IoSocket_init,
    .tp_repr                                          = (reprfunc)IoSocket_repr,
    .tp_dealloc                                       = (destructor)IoSocket_dealloc,
    .tp_members                                       = IoSocket_members,
};

static int ymq_exec(PyObject* module) {
    if (PyType_Ready(&IoSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoSocket", (PyObject*)&IoSocketType) < 0)
        return -1;

    // Additional initialization code can go here

    return 0;
}

// just a dummy function to demonstrate the module structure
static PyObject* myrandom(PyObject* self, PyObject* args) {
    return PyLong_FromLong(7);
}

static PyMethodDef ymq_methods[] = {
    {"random", myrandom, METH_NOARGS, "Generate a random number"}, {NULL, NULL, 0, NULL}  // Sentinel
};

static PyModuleDef_Slot ymq_slots[] = {
    {Py_mod_exec, ymq_exec},
    // todo: this has an import error for some reason
    // {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED},
    {0, NULL}  // Sentinel
};

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = "YMQ Python bindings",
    .m_size    = 0,
    .m_methods = ymq_methods,
    .m_slots   = ymq_slots,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}
