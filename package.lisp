;;;; package.lisp

(defpackage #:cl-rados
  (:use #:cl #:cffi #:flexi-streams #:trivial-gray-streams #:let-over-lambda)
  (:export :with-open-cephfile
           :ceph-open
           :with-rados
           :write-string-to-ceph
           :write-octets-to-ceph
           :dump-ceph-obj-to-file
           :dump-file-to-ceph-obj))

(defpackage #:cl-rados.test
  (:use #:cl #:cl-rados #:flexi-streams))
