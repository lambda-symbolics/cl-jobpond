(in-package #:cl-jobpond)

;;;; -- Conditions --

(define-condition job-pool-error (error)
  ((message
    :initarg :message
    :reader job-pool-error-message
    :type string
    :documentation "The human-readable description of the refused request."))
  (:report
   (lambda (condition stream)
     (write-string (job-pool-error-message condition) stream)))
  (:documentation "A job pool refused a submission, lookup, or lifecycle request."))

(define-condition job-pool-invalid-limit (job-pool-error)
  ((limit-kind
    :initarg :limit-kind
    :reader job-pool-invalid-limit-kind
    :type keyword
    :documentation "The limit that was given an unusable value.")
   (value
    :initarg :value
    :reader job-pool-invalid-limit-value
    :documentation "The rejected limit value."))
  (:documentation "A pool limit received a value outside its supported range."))

(define-condition job-pool-invalid-entry (job-pool-error)
  ((entry
    :initarg :entry
    :reader job-pool-invalid-entry-entry
    :documentation "The rejected batch entry."))
  (:documentation "A submission entry did not describe a runnable job."))

(define-condition job-pool-closed (job-pool-error)
  ((lifecycle-state
    :initarg :lifecycle-state
    :reader job-pool-closed-lifecycle-state
    :type keyword
    :documentation "The pool lifecycle state that refused the request."))
  (:documentation "A pool that is closing or closed cannot admit further work."))

(define-condition job-pool-capacity-exceeded (job-pool-error)
  ((limit-kind
    :initarg :limit-kind
    :reader job-pool-capacity-exceeded-limit-kind
    :type keyword
    :documentation "The :BATCH-SIZE or :LIVE-JOBS bound the request would break.")
   (limit
    :initarg :limit
    :reader job-pool-capacity-exceeded-limit
    :type (integer 1)
    :documentation "The bound in force when the request arrived.")
   (requested-count
    :initarg :requested-count
    :reader job-pool-capacity-exceeded-requested-count
    :type (integer 0)
    :documentation "The number of jobs the refused request asked to admit.")
   (live-count
    :initarg :live-count
    :initform 0
    :reader job-pool-capacity-exceeded-live-count
    :type (integer 0)
    :documentation "The queued and running jobs already admitted by the pool."))
  (:documentation "Admitting a request would exceed a hard pool bound."))

(define-condition job-not-found (job-pool-error)
  ((identifier
    :initarg :identifier
    :reader job-not-found-identifier
    :type string
    :documentation "The identifier that no live or retained job carries."))
  (:documentation "A job identifier names no live or retained job."))

(define-condition job-pool-detach-refused (job-pool-error)
  ((reason
    :initarg :reason
    :reader job-pool-detach-refused-reason
    :type keyword
    :documentation "The :NOT-CLOSED or :THREADS-ALIVE cause of the refusal."))
  (:documentation "A pool cannot drop its runtime state while it is still live."))

(define-condition job-aborted (serious-condition)
  ((message
    :initarg :message
    :reader job-aborted-message
    :type string
    :documentation "The concise cancellation explanation retained on the job.")
   (identifier
    :initarg :identifier
    :reader job-aborted-identifier
    :type string
    :documentation "The identifier of the job whose run was interrupted.")
   (reason
    :initarg :reason
    :reader job-aborted-reason
    :type keyword
    :documentation "The structured reason the run was interrupted."))
  (:report
   (lambda (condition stream)
     (write-string (job-aborted-message condition) stream)))
  (:documentation
   "The control condition unwinding a deliberately interrupted job body.

This is a SERIOUS-CONDITION rather than an ERROR so that a job body handling
ERROR broadly cannot accidentally swallow its own cancellation."))
