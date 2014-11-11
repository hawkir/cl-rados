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
                        do (write-byte (print (mem-aref *res :uchar i)) dumpfile))
                     num-bytes)
           until (= 0 n))
           ;;uncomment this to get a summary of the bytes read in each pass
           ;; collect n
        pos))))

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
                                file-id string (length (string-to-octets string)))
              0)))

(defun main ()
  (with-rados (cluster io
                       :id "platform"
                       :keyring "/etc/ceph/ceph.client.admin.keyring"
                       :conf-file "/home/rick/Downloads/ceph.conf"
                       :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io "iTunes__iTunes_AU__80030548_0114_AU.txt.gz" "~/bar"  :chunk-size 3)))

(defun test-read (ceph-id)
  (with-rados (cluster io
                       :id "platform"
                       :keyring "/etc/ceph/ceph.client.admin.keyring"
                       :conf-file "/home/rick/Downloads/ceph.conf"
                       :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 1000)))

(defun binary-garbage-loopback (ceph-id)
  (with-rados (cluster io
                       :id "platform"
                       :keyring "/etc/ceph/ceph.client.admin.keyring"
                       :conf-file "/home/rick/Downloads/ceph.conf"
                       :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (write-octets-to-ceph io ceph-id (make-array 256 :initial-contents (loop for i from 0 to 255 collect i)))
    (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 2)))
