%module bindings


%insert("lisphead") %{
(in-package :cl-alsaseq)
%}


%include "/usr/include/rados/librados.h"
/* %include "/usr/include/stdio.h" */
/* %include "/usr/include/stdlib.h" */
