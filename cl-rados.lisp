;;;; cl-rados.lisp

(in-package #:cl-rados)

;;; "cl-rados" goes here. Hacks and glory await!
(load-foreign-library "librados.so")

(defmacro! with-rados ((io &key id keyring conf-file pool-name) &body body)
  (assert (and id keyring conf-file pool-name))
  `(with-foreign-object (,g!*cluster :pointer)
     (assert (>= (rados_create ,g!*cluster ,id) 0))
     (let ((,g!cluster (mem-ref ,g!*cluster :pointer)))
       (unwind-protect
            (progn 
              (assert (>= (rados_conf_set ,g!cluster
                                          "keyring"
                                          ,keyring)
                          0))
              (assert (>= (rados_conf_read_file ,g!cluster
                                                ,conf-file)
                          0))
              (assert (>= (rados_connect ,g!cluster)
                          0))
              (with-foreign-object (,g!*io :pointer)
                (progn
                  (assert (>= (rados_ioctx_create ,g!cluster
                                                  ,pool-name
                                                  ,g!*io)
                              0))
                  (let ((,io (mem-ref ,g!*io :pointer)))
                    (unwind-protect
                         (progn ,@body)
                      (rados_ioctx_destroy ,io ))))))
         (rados_shutdown ,g!cluster)))))

(defun write-octets-to-ceph (io file-id octets)
  (let ((bytes (make-array (length octets)
                           :element-type '(unsigned-byte 8)
                           :initial-contents octets)))
    (with-pointer-to-vector-data (*bytes bytes)
      (assert (>= (rados_write_full io
                                    file-id *bytes (length bytes))
                  0)))))

(defun write-string-to-ceph (io file-id string)
  (assert (>= (rados_write_full io
                                file-id string (length (string-to-octets string :external-format :utf8)))
              0)))

(defun dump-ceph-obj-to-file (io file-id filespec &key (chunk-size 1000))
  (with-open-file (dumpfile filespec :direction :output
                            :if-exists :supersede
                            :if-does-not-exist :create
                            :element-type 'octet)
    (with-foreign-object (*res :uchar chunk-size)
      (let ((pos 0))
        (loop
           for n = (let ((num-bytes (rados_read io
                                                file-id *res chunk-size pos)))
                     (assert (>= num-bytes 0))
                     (incf pos num-bytes)
                     (loop for i below num-bytes
                        do (write-byte (mem-aref *res :uchar i) dumpfile))
                     num-bytes)
           until (= 0 n))
           ;;uncomment this to get a summary of the bytes read in each pass
           ;; collect n
        pos))))

(defun ceph-open-output (ceph-id ioctx &key (element-type 'base-char)
                                         if-exists if-does-not-exist (external-format :default))
  (declare (ignore if-exists if-does-not-exist))
  (let ((binary-stream (make-instance 'ceph-binary-output-stream
                                      :ioctx ioctx
                                      :ceph-id ceph-id)))
    (cond
      ((subtypep element-type 'base-char) (make-flexi-stream binary-stream
                                                             :external-format external-format))
      ((subtypep element-type 'integer) binary-stream)
      (t (error "unknown type ~S supplied to ceph-open-output" element-type)))))

(defun ceph-open-input (ceph-id ioctx &key (element-type 'base-char) (external-format :utf8))
  (let ((binary-stream  (make-instance 'ceph-binary-input-stream
                                       :ioctx ioctx
                                       :ceph-id ceph-id)))
    (cond
      ((subtypep element-type 'base-char) (make-flexi-stream binary-stream
                                                             :external-format external-format))
      ((subtypep element-type 'integer) binary-stream)
      (t (error "unknown type ~S supplied to ceph-open-input" element-type)))))

(defun ceph-open (ceph-id ioctx &key (direction :input) (element-type 'base-char)
                             if-exists if-does-not-exist (external-format :utf8))
  (case direction
    (:output (ceph-open-output ceph-id ioctx :element-type element-type
                               :if-exists if-exists
                               :if-does-not-exist if-does-not-exist
                               :external-format external-format))
    (:input (ceph-open-input  ceph-id ioctx :element-type element-type
                              :external-format  external-format))))

(defmacro with-open-cephfile ((stream ceph-id ioctx &rest options &key
                                      (direction :input)
                                      (element-type ''base-char)
                                      if-exists if-does-not-exist
                                      (external-format :utf8))
                              &body body)
  `(let ((,stream (ceph-open ,ceph-id ,ioctx
                             :direction ,direction
                             :element-type ,element-type
                             :if-exists ,if-exists
                             :if-does-not-exist ,if-does-not-exist
                             :external-format ,external-format ,@options)))
     (unwind-protect
          (progn ,@body)
       (close ,stream))))
