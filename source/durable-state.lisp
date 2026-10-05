(in-package #:cl-jobpond)

;;;; -- Portable Transactional State --

(export '(durable-state-error durable-state-error-reason durable-state-error-identity))

(define-condition durable-state-error (error)
  ((reason :initarg :reason :reader durable-state-error-reason
           :documentation "The rejected transition or invalid field.")
   (identity :initarg :identity :initform nil :reader durable-state-error-identity
             :documentation "The schedule or message identity, when known."))
  (:report (lambda (condition stream)
             (format stream "Durable state rejected ~S for ~S."
                     (durable-state-error-reason condition)
                     (durable-state-error-identity condition))))
  (:documentation "A portable-state validation or lifecycle failure."))

(defun durable-state--fail (reason &optional identity)
  "Signal a typed state failure with REASON and IDENTITY."
  (error 'durable-state-error :reason reason :identity identity))

(defun durable-state--copy (value)
  "Copy bounded portable data, refusing cycles, excessive depth and opaque values."
  (let ((active (make-hash-table :test 'eq)) (nodes 0))
    (labels ((copy-value (value depth)
               (when (or (> (incf nodes) 100000) (> depth 64))
                 (durable-state--fail ':payload-limit))
               (typecase value
                 (null nil)
                 (string (when (> (length value) 1048576)
                           (durable-state--fail ':payload-limit))
                         (copy-seq value))
                 (rational value)
                 (keyword value)
                 (cons
                  (let ((marked nil))
                    (unwind-protect
                         (loop for tail = value then (rest tail)
                               while (consp tail)
                               do (when (gethash tail active)
                                    (durable-state--fail ':cyclic-data))
                                  (when (> (incf nodes) 100000)
                                    (durable-state--fail ':payload-limit))
                                  (setf (gethash tail active) t)
                                  (push tail marked)
                               collect (copy-value (first tail) (1+ depth))
                               into result
                               finally (unless (null tail)
                                         (durable-state--fail ':improper-list))
                                       (return result))
                      (dolist (cell marked) (remhash cell active)))))
                 (t (if (eq value t) t (durable-state--fail ':nonportable-data))))))
      (copy-value value 0))))

(defun durable-state--identifier (value)
  "Validate a stable bounded string identity."
  (unless (and (stringp value) (<= 1 (length value) 256))
    (durable-state--fail ':identity))
  value)

(defclass durable-state ()
  ((lock :initform (make-lock "Durable state") :reader durable-state-lock
         :documentation "Serializes transactions and snapshots.")
   (data :initarg :data :accessor durable-state-data
         :documentation "Private portable state, replaced after each successful commit.")
   (healthy-p :initform t :accessor durable-state--healthy-p
              :documentation "False after a nonlocal escape from a possibly committed store write.")
   (store :initarg :store :reader durable-state-store
          :documentation "Atomic durable snapshot writer, or NIL for memory-only state.")
   (clock :initarg :clock :reader durable-state-clock
          :documentation "Function returning nonnegative integer time units."))
  (:documentation "A single-writer state with persistence before visibility."))

(defgeneric durable-state--changed (state)
  (:documentation "Notify runtime waiters after publication while holding STATE's lock."))

(defmethod durable-state--changed ((state durable-state))
  nil)

(defun durable-state--now (state)
  "Read STATE's injected integer clock."
  (let ((now (funcall (durable-state-clock state))))
    (unless (typep now '(integer 0 *)) (durable-state--fail ':clock))
    now))

(defun durable-state--transaction (state function)
  "Commit FUNCTION's changes atomically; return its values after durable publication.
FUNCTION receives an isolated copy. STORE runs under the lock and must not reenter.
If STORE signals, the live state is unchanged; its external write outcome is owned
by the store implementation, which must provide atomic replace semantics."
  (with-lock-held ((durable-state-lock state))
    (unless (durable-state--healthy-p state) (durable-state--fail ':uncertain-store))
    (let* ((data (durable-state--copy (durable-state-data state)))
           (results (multiple-value-list (funcall function data))))
      ;; Isolate newly inserted caller data and validate every returned value before persistence.
      (setf data (durable-state--copy data)
            results (mapcar #'durable-state--copy results))
      (when (durable-state-store state)
        (setf (durable-state--healthy-p state) nil)
        (handler-case
            (funcall (durable-state-store state) (durable-state--copy data))
          (error (condition)
            ;; An ordinary store refusal promises unchanged durable storage.
            (setf (durable-state--healthy-p state) t)
            (error condition))))
      (setf (durable-state-data state) data
            (durable-state--healthy-p state) t)
      (durable-state--changed state)
      (values-list results))))

(defun durable-state--snapshot (state)
  "Return an isolated coherent snapshot of STATE."
  (with-lock-held ((durable-state-lock state))
    (unless (durable-state--healthy-p state) (durable-state--fail ':uncertain-store))
    (durable-state--copy (durable-state-data state))))

(defun durable-state--record (data key identity)
  "Find IDENTITY in DATA's KEY records, or signal a typed missing error."
  (or (find identity (getf data key) :key (lambda (record) (getf record :id))
            :test #'equal)
      (durable-state--fail ':not-found identity)))

(defun durable-state--token (data)
  "Allocate a persisted monotonically increasing claim token."
  (incf (getf data :sequence)))


(defun durable-state--plist (value keys)
  "Require exactly KEYS once each in a proper portable property list."
  (unless (and (listp value) (= (length value) (* 2 (length keys)))
               (let ((seen nil))
                 (loop for (key datum) on value by #'cddr
                       always (and (member key keys) (not (member key seen))
                                   (progn (push key seen) t)))))
    (durable-state--fail ':snapshot))
  value)
