# encoding:utf-8
# distutils: language=c++

from libc.string cimport const_char

cdef extern from "DataCollect.h":
    int CTP_GetSystemInfo(char *pSystemInfo, int &nLen) except + nogil
    const_char *CTP_GetDataCollectApiVersion() except + nogil
