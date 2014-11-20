(in-package :cl-rados)

(defclass trivial-character-stream ()
  ((external-format :initarg :external-format
                    :initform :latin1
                    :accessor external-format)
   (charbuf :initform (make-array *default-buffer-size*
                                  :element-type 'character
                                  :fill-pointer 0)
            :accessor charbuf)))

(defclass trivial-character-input-stream (trivial-character-stream fundamental-character-input-stream)
  ())

(defun fill-charbuf (buffer new-contents)
  (let ((count (length new-contents)))
    (setf (fill-pointer buffer) count)
    (loop for i below count
       do
         (setf (aref buffer i)
               (aref new-contents i)))
    count))

(defun %stream-read-byte (stream)
  (let ((byte (stream-read-byte stream)))
    (if (eq byte :EOF)
        (error 'end-of-file)
        byte)))

(defmethod stream-fill-charbuf ((stream trivial-character-input-stream))
  (let ((byte-buf (make-array *default-buffer-size*
                              :element-type '(unsigned-byte 8)
                              :fill-pointer 0)))
    (handler-case
        (progn
          (loop repeat (- *default-buffer-size* 4)
             do (vector-push (%stream-read-byte stream) byte-buf))
          (loop repeat 4
             do
               (return-from stream-fill-charbuf
                 (fill-charbuf (charbuf stream)
                               (reverse (handler-case
                                            (progn
                                              (vector-push (%stream-read-byte stream) byte-buf)
                                              (octets-to-string byte-buf :external-format (external-format stream)))
                                          (external-format-encoding-error (err)
                                            (error 'rados-external-format-encoding-error
                                                   :format-control "bad data encountered in stream whilst trying to decode as ~a because:~%~a"
                                                   :format-arguments (list (external-format stream) err)))))))))
      (end-of-file ()
        (fill-charbuf (charbuf stream)
                      (reverse (octets-to-string byte-buf :external-format (external-format stream))))))))
    
(define-condition rados-external-format-encoding-error (simple-condition)
   ((message :initarg :message :accessor rados-external-format-encoding-error)))

(defmethod stream-read-char ((stream trivial-character-input-stream))
  (let ((buffer (charbuf stream)))
    (if (= 0 (fill-pointer buffer))
        (handler-case
            (progn
              (stream-fill-charbuf stream))
          (end-of-file ())))
    (if (= 0 (fill-pointer buffer))
        :EOF
        (vector-pop buffer))))

(defmethod stream-unread-char ((stream trivial-character-input-stream) char)
  (vector-push-extend char (charbuf stream)))

(defclass trivial-character-output-stream (trivial-character-stream fundamental-character-output-stream)
  ())

(defmethod stream-drain-charbuf ((stream trivial-character-output-stream))
  (let ((buffer (charbuf stream)))
    (let ((octets (string-to-octets buffer
                                    :external-format (external-format stream))))
      (loop for i below (length octets)
         do (stream-write-byte stream (aref octets i)))
      (setf (fill-pointer buffer) 0))))

(defmethod stream-write-char ((stream trivial-character-output-stream) char)
  (let ((buffer (charbuf stream)))
    (if (= *default-buffer-size* (fill-pointer buffer))
        (stream-drain-charbuf stream))
    (vector-push char buffer)))

(defmethod stream-write-sequence ((stream trivial-character-output-stream) sequence start end &rest rest)
  (apply #'call-next-method `(,stream ,(string-to-octets sequence) ,start ,end ,@rest)))

(defmethod stream-force-output ((stream trivial-character-output-stream))
  (stream-drain-charbuf stream)
  (stream-drain-buffer stream))
