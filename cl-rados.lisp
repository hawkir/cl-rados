;;;; cl-rados.lisp

(in-package #:cl-rados)

;;; "cl-rados" goes here. Hacks and glory await!
(load-foreign-library "librados.so")


;; int main (int argc, char* argv []) {
(defun main ()
  ;; int err;
  ;; rados_t cluster;
  (with-foreign-object (*cluster :pointer)
;;   err = rados_create(&cluster, "platform");
;;   if (err < 0) {
;;     fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
;;     exit(1);
;;   }
    (assert (>= (rados_create *cluster "platform") 0))

;;   err = rados_conf_set(cluster, "keyring", "/etc/ceph/ceph.client.admin.keyring");
;;   if (err < 0) {
;;     fprintf(stderr, "%s: keyring failed because: %s\n", argv[0], strerror(-err));
;;     exit(1);
    ;;   }
    (assert (>= (rados_conf_set (mem-ref *cluster :pointer)
                                "keyring"
                                "/etc/ceph/ceph.client.admin.keyring")
                0))
   
;;   err = rados_conf_read_file(cluster, "/home/rick/Downloads/ceph.conf");
;;   if (err < 0) {
;;     fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
;;     exit(1);
    ;;   }
    (assert (>= (rados_conf_read_file (mem-ref *cluster :pointer)
                                      "/home/rick/Downloads/ceph.conf")
                0))
;;   err = rados_connect(cluster);
;;   if (err < 0) {
;;     fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
;;     exit(1);
;;   }
;;   rados_ioctx_t io;
;;   char *poolname = "platform";
    (assert (>= (rados_connect (mem-ref *cluster :pointer))))
    

;;   err = rados_ioctx_create(cluster, poolname, &io);
;;   if (err < 0) {
;;     fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
;;     rados_shutdown(cluster);
;;     exit(1);
    ;;   }
    (with-foreign-object (*io :pointer)
      (assert (>= (rados_ioctx_create (mem-ref *cluster :pointer)
                                      "platform"
                                      *io)
                  0))
;;   err = rados_write_full(io, "greeting", "hello", 5);
;;   if (err < 0) {
;;     fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
;;     rados_ioctx_destroy(io);
;;     rados_shutdown(cluster);
;;     exit(1);
;;   }
      (assert (>= (rados_write_full (mem-ref *io :pointer)
                                    "greeting" "hello" 5)
                  0))
;;   char res[1000];
      (with-foreign-object (*res :char 1000)
;;   err = rados_read(io, "greeting", res, 1000, 0);
        (assert (>= (rados_read (mem-ref *io :pointer)
                              "greeting" *res 1000 0)))
;;   if (err < 0) {
;;     fprintf(stderr, "%s: failed to read from pool %s: %s\n", argv[0], poolname, strerror(-err));
;;     rados_ioctx_destroy(io);
;;     rados_shutdown(cluster);
;;     exit(1);
        ;;   }
        
;;   printf("read back %s\n\n", res);
;;   rados_ioctx_destroy(io);
;;   rados_shutdown(cluster);
        (rados_ioctx_destroy (mem-ref *io :pointer))
        (rados_shutdown (mem-ref *cluster :pointer))))))
;; }

