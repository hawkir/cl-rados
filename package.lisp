;;;; package.lisp

(defpackage #:cl-rados
  (:use #:cl #:cffi #:flexi-streams #:trivial-gray-streams #:let-over-lambda)
  (:export :with-open-cephfile
           :ceph-open
           :with-rados
           :dump-ceph-obj-to-file))
