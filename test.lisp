(in-package :cl-rados.test)

(defmacro with-test-rados ((io) &body body)
  `(with-rados (,io
                :id "platform"
                :keyring "/etc/ceph/ceph.client.platform.keyring"
                :conf-file "/etc/ceph/ceph.platform.conf"
                :pool-name "platform")
     ,@body))

(defun main ()
  (with-test-rados (io)
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io "iTunes__iTunes_AU__80030548_0114_AU.txt.gz" "~/bar"  :chunk-size 3)))

(defun test-read (ceph-id)
  (with-test-rados (io)
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 1000)))

(defun binary-garbage-loopback (ceph-id &optional (num-bytes 50000000))
  (with-test-rados (io)
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (write-octets-to-ceph io ceph-id (make-array num-bytes
                                                 :initial-contents (loop for i from 1 to num-bytes
                                                                      collect (random 256))))
    (print "made junk")
    (time (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 100000))))

(defun monkey ()
  (with-test-rados (io)
    (with-open-cephfile (cephout "test123" io :direction :output)
      (format cephout "monkey~%monkey~%"))
    (with-open-cephfile (cephin "test123" io)
      (loop repeat 20 do (print (read-line cephin nil))))))

(defun gorilla ()
  (with-test-rados (io)
    (with-open-cephfile (gzstream "iTunes__iTunes_US__report__80030548_0914_US.txt.gz" io :direction :input :element-type 'octet)
      
      (let* ((bin-stream (chipz:make-decompressing-stream :gzip gzstream))
             (txt-stream (make-flexi-stream bin-stream :external-format :utf8)))
        (loop for i from 0 to 10000 collect (read-line txt-stream nil))))))
