(in-package :cl-rados.test)



(defun main ()
  (with-rados (io
               :id "platform"
               :keyring "/etc/ceph/ceph.client.admin.keyring"
               :conf-file "/home/rick/Downloads/ceph.conf"
               :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io "iTunes__iTunes_AU__80030548_0114_AU.txt.gz" "~/bar"  :chunk-size 3)))

(defun test-read (ceph-id)
  (with-rados (io
               :id "platform"
               :keyring "/etc/ceph/ceph.client.admin.keyring"
               :conf-file "/home/rick/Downloads/ceph.conf"
               :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 1000)))

(defun binary-garbage-loopback (ceph-id &optional (num-bytes 50000000))
  (with-rados (io
               :id "platform"
               :keyring "/etc/ceph/ceph.client.admin.keyring"
               :conf-file "/home/rick/Downloads/ceph.conf"
               :pool-name "platform")
    ;; (write-string-to-ceph io "greeting" "foobarbaz")
    (write-octets-to-ceph io ceph-id (make-array num-bytes
                                                 :initial-contents (loop for i from 1 to num-bytes
                                                                      collect (random 256))))
    (print "made junk")
    (time (dump-ceph-obj-to-file io ceph-id
                           (concatenate 'string "/home/rick/ceph-test/" ceph-id)
                           :chunk-size 100000))))
