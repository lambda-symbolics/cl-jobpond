(in-package #:cl-jobpond/tests)

(defvar *test-count* 0
  "The number of assertions executed by the current test run.")

(defun test-assert (condition description)
  "Record and require CONDITION for DESCRIPTION."
  (incf *test-count*)
  (unless condition
    (error "cl-jobpond test failed: ~A" description))
  t)

(defmacro signals (condition-type &body body)
  "Return true when BODY signals CONDITION-TYPE."
  `(handler-case
       (progn ,@body nil)
     (,condition-type () t)))
