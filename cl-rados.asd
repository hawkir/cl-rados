;;;; cl-rados.asd

(asdf:defsystem #:cl-rados
  :serial t
  :description "Describe cl-rados here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:babel #:flexi-streams #:trivial-gray-streams #:let-over-lambda #:cffi)
  :components ((:file "package")
               (:file "bindings")
               (:file "cl-rados")
               (:file "graystream")
               (:file "test")))

