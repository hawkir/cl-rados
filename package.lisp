;;;; package.lisp

(defpackage #:cl-rados
  (:use #:cl #:cffi #:flexi-streams #:trivial-gray-streams #:let-over-lambda))

(defpackage #:cl-rados.graystream-example
  (:use :cl :cl-rados #:trivial-gray-streams))
