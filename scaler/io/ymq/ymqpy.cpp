#define PY_SSIZE_T_CLEAN
#include <Python.h>

// just a dummy function to demonstrate the module structure
static PyObject* myrandom(PyObject* self, PyObject* args) {
    return PyLong_FromLong(7);
}

static PyMethodDef ymq_methods[] = {
    {"random", myrandom, METH_NOARGS, "Generate a random number"},
    {NULL, NULL, 0, NULL}  // Sentinel
};

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = "YMQ Python bindings",
    .m_size    = 0,
    .m_methods = ymq_methods,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}
