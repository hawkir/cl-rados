;;;; cl-rados.lisp

(in-package #:cl-rados)

;;; "cl-rados" goes here. Hacks and glory await!
(load-foreign-library "librados.so")


(defmacro! with-rados ((cluster io &key id keyring conf-file pool-name) &body body)
  (assert (and id keyring conf-file pool-name))
  `(with-foreign-object (,g!*cluster :pointer)
     (assert (>= (rados_create ,g!*cluster ,id) 0))
     (let ((,cluster (mem-ref ,g!*cluster :pointer)))
       (unwind-protect
            (progn 
              (assert (>= (rados_conf_set ,cluster
                                          "keyring"
                                          ,keyring)
                          0))
              (assert (>= (rados_conf_read_file ,cluster
                                                ,conf-file)
                          0))
              (assert (>= (rados_connect ,cluster)))
              (with-foreign-object (,g!*io :pointer)
                (progn
                  (assert (>= (rados_ioctx_create ,cluster
                                                  ,pool-name
                                                  ,g!*io)
                              0))
                  (let ((,io (mem-ref ,g!*io :pointer)))
                    (unwind-protect
                         (progn ,@body)
                      (rados_ioctx_destroy ,io ))))))
         (rados_shutdown ,cluster)))))
       

(defun dump-ceph-obj-to-file (cluster io file-id filespec)
  (with-foreign-object (*res :uchar 1000)
    (with-open-file (dumpfile filespec :direction :output
                              :if-exists :supersede
                              :if-does-not-exist :create
                              :element-type 'octet)
      (let ((num-bytes (print (rados_read io
                                          file-id *res 1000 0))))
        (assert (>= num-bytes 0))
        (print (loop for i below num-bytes
                  do (write-byte (mem-aref *res :uchar i) dumpfile)))))))

(defun write-string-to-ceph (cluster io file-id string)
  (assert (>= (rados_write_full io
                                file-id string (length (string-to-octets string)))
              0)))

;; int main (int argc, char* argv []) {
(defun main ()
  (with-rados (cluster io
                       :id "platform"
                       :keyring "/etc/ceph/ceph.client.admin.keyring"
                       :conf-file "/home/rick/Downloads/ceph.conf"
                       :pool-name "platform")
    (write-string-to-ceph cluster io "greeting" "heyheyhey")
    (dump-ceph-obj-to-file cluster io "greeting" "~/bar")))
