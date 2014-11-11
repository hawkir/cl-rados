%module bindings


%insert("lisphead") %{
(in-package :cl-rados)
%}


%include "/usr/include/rados/librados.h"
/* %include "/usr/include/sys/types.h" */
%include "stdint.i"

typedef unsigned int size_t;
/* typedef unsigned long uint64_t; */
%include "/usr/include/rados/rados_types.h"

/* %include "/usr/include/stdio.h" */
/* %include "/usr/include/stdlib.h" */
