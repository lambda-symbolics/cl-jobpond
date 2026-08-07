(in-package #:cl-jobpond)

;;;; -- Pool Policy --

(defparameter *default-maximum-concurrency* 8
  "The default number of jobs a new pool may run at the same time.")

(defparameter *maximum-concurrency-limit* 32
  "The largest reusable worker count a pool accepts.")

(defparameter *default-maximum-batch-size* 16
  "The default number of jobs accepted in one atomic admission.")

(defparameter *default-maximum-live-jobs* 64
  "The default maximum combined queued and running jobs of one pool.")

(defparameter *default-maximum-runtime-milliseconds* 0
  "The default wall-clock cap for one job, where zero disables deadlines.")

(defparameter *default-terminal-retention-limit* 64
  "The default number of terminal jobs a pool keeps for later inspection.")

(defparameter *shutdown-timeout-seconds* 10
  "The time a pool allows its reusable threads to stop during close.")

(defparameter *monitor-poll-seconds* 0.1
  "The interval at which the deadline monitor rechecks running jobs.")

(defparameter *progress-output-limit* 8000
  "The streamed output characters a live job retains as a bounded tail.")

(defparameter *retained-progress-output-limit* 1000
  "The streamed output characters a terminal job keeps after compaction.")

(defparameter *retained-report-limit* 2000
  "The characters retained from a condition report on a terminal job.")

(defparameter *identifier-fragment-limit* 64
  "The characters of a caller-supplied name reused in a job identifier.")


;;;; -- Worker Thread State --

(defvar *current-job* nil
  "The job the calling reusable worker is running, or NIL.")

(defvar *current-run-token* nil
  "The run token guarding cancellation interrupts on the calling worker.")

(defvar *terminal-publication-job* nil
  "The job protected from a delayed interrupt while it publishes its result.")

(defvar *run-token-lock* (make-lock "cl-jobpond run tokens")
  "The lock serializing run token allocation across every pool.")

(defvar *run-token-index* 0
  "The monotonically increasing source of unique run tokens.")


;;;; -- Classes --

(defclass job-progress ()
  ((lock
    :initform (make-lock "cl-jobpond job progress")
    :reader job-progress--lock
    :documentation "The lock protecting progress fields read by pool clients.")
   (state
    :initform :queued
    :accessor job-progress--state
    :type keyword
    :documentation "The mirrored :QUEUED, :RUNNING, or terminal job state.")
   (detail
    :initform nil
    :accessor job-progress--detail
    :documentation "The newest progress value the job body reported.")
   (output-tail
    :initform ""
    :accessor job-progress--output-tail
    :type string
    :documentation "The bounded tail of output the job body streamed.")
   (step-count
    :initform 0
    :accessor job-progress--step-count
    :type (integer 0)
    :documentation "The progress steps the job body has reported.")
   (started-at
    :initform nil
    :accessor job-progress--started-at
    :documentation "The internal real time execution began, or NIL.")
   (updated-at
    :initform (get-internal-real-time)
    :accessor job-progress--updated-at
    :type integer
    :documentation "The internal real time of the newest progress event."))
  (:documentation "The normalized thread-safe progress record of one job."))

(defclass job-pool ()
  ((name
    :initarg :name
    :reader job-pool-name
    :type string
    :documentation "The human-readable pool name used for thread names.")
   (lock
    :initform (make-lock "cl-jobpond job pool")
    :reader job-pool--lock
    :documentation "The lock protecting queue, jobs, threads, and listeners.")
   (condition-variable
    :initform (make-condition-variable)
    :reader job-pool--condition-variable
    :documentation "The condition waking reusable workers and close waiters.")
   (maximum-concurrency
    :initarg :maximum-concurrency
    :accessor job-pool-maximum-concurrency
    :type (integer 1)
    :documentation "The maximum jobs that may execute at the same time.")
   (maximum-batch-size
    :initarg :maximum-batch-size
    :accessor job-pool-maximum-batch-size
    :type (integer 1)
    :documentation "The maximum jobs accepted in one atomic admission.")
   (maximum-live-jobs
    :initarg :maximum-live-jobs
    :accessor job-pool-maximum-live-jobs
    :type (integer 1)
    :documentation "The maximum combined queued and running jobs.")
   (maximum-runtime-milliseconds
    :initarg :maximum-runtime-milliseconds
    :accessor job-pool-maximum-runtime-milliseconds
    :type (integer 0)
    :documentation "The default wall-clock cap for one job, or zero when off.")
   (job-class
    :initarg :job-class
    :reader job-pool-job-class
    :type symbol
    :documentation "The class this pool instantiates for each admitted job.")
   (terminal-retention-limit
    :initarg :terminal-retention-limit
    :accessor job-pool-terminal-retention-limit
    :type (integer 1)
    :documentation "The number of terminal jobs retained for inspection.")
   (queue
    :initform nil
    :accessor job-pool--queue
    :type list
    :documentation "The bounded first-in first-out queue of admitted jobs.")
   (worker-threads
    :initform nil
    :accessor job-pool--worker-threads
    :type list
    :documentation "The reusable worker threads owned by this pool.")
   (monitor-thread
    :initform nil
    :accessor job-pool--monitor-thread
    :documentation "The single deadline monitor thread, or NIL.")
   (shutdown-p
    :initform nil
    :accessor job-pool--shutdown-p
    :type boolean
    :documentation "True while admission is closed and workers must exit.")
   (lifecycle-state
    :initform :open
    :accessor job-pool-lifecycle-state
    :type keyword
    :documentation "The :OPEN, :CLOSING, or :CLOSED pool lifecycle state.")
   (close-owner
    :initform nil
    :accessor job-pool--close-owner
    :documentation "The thread coordinating shutdown, or NIL between attempts.")
   (active-count
    :initform 0
    :accessor job-pool--active-count
    :type (integer 0)
    :documentation "The jobs currently executing on reusable workers.")
   (live-count
    :initform 0
    :accessor job-pool--live-count
    :type (integer 0)
    :documentation "The admitted queued, running, and publishing jobs.")
   (next-index
    :initform 0
    :accessor job-pool--next-index
    :type (integer 0)
    :documentation "The monotonically increasing job index source.")
   (jobs
    :initform (make-hash-table :test #'equal)
    :accessor job-pool--jobs
    :type hash-table
    :documentation "Job identifiers mapped to live and retained terminal jobs.")
   (terminal-identifiers
    :initform nil
    :accessor job-pool--terminal-identifiers
    :type list
    :documentation "Terminal job identifiers ordered from oldest to newest.")
   (listeners
    :initform nil
    :accessor job-pool--listeners
    :type list
    :documentation "Callbacks receiving portable lifecycle and progress events."))
  (:documentation "A bounded pool of reusable workers running supervised jobs."))

(defclass job ()
  ((pool
    :initarg :pool
    :reader job-pool
    :type job-pool
    :documentation "The pool that admitted and supervises this job.")
   (identifier
    :initarg :identifier
    :reader job-identifier
    :type string
    :documentation "The unique identifier this pool assigned to the job.")
   (index
    :initarg :index
    :reader job-index
    :type (integer 1)
    :documentation "The admission order of this job within its pool.")
   (name
    :initarg :name
    :reader job-name
    :type (or null string)
    :documentation "The caller-supplied descriptive name, or NIL.")
   (payload
    :initarg :payload
    :reader job-payload
    :documentation "The caller-supplied value the job body may consult.")
   (owner-identifiers
    :initarg :owner-identifiers
    :reader job-owner-identifiers
    :type list
    :documentation "The identifiers of this job's ancestors, outermost first.")
   (root-identifier
    :initarg :root-identifier
    :reader job-root-identifier
    :type (or null string)
    :documentation "The identifier naming the tree this job belongs to, or NIL.")
   (body-function
    :initarg :body-function
    :reader job--body-function
    :type function
    :documentation "The caller-supplied body, called with the job as argument.")
   (maximum-runtime-milliseconds
    :initarg :maximum-runtime-milliseconds
    :reader job-maximum-runtime-milliseconds
    :type (integer 0)
    :documentation "The wall-clock cap for this job, or zero when disabled.")
   (lock
    :initform (make-lock "cl-jobpond job")
    :reader job--lock
    :documentation "The lock protecting mutable job lifecycle fields.")
   (condition-variable
    :initform (make-condition-variable)
    :reader job--condition-variable
    :documentation "The condition waking waiters after a state transition.")
   (state
    :initform :queued
    :accessor job-state
    :type keyword
    :documentation "The :QUEUED, :RUNNING, :COMPLETED, :FAILED, or :ABORTED state.")
   (publication-claimed-p
    :initform nil
    :accessor job--publication-claimed-p
    :type boolean
    :documentation "True while one writer prepares the terminal publication.")
   (thread
    :initform nil
    :accessor job--thread
    :documentation "The reusable worker currently running this job, or NIL.")
   (run-token
    :initform nil
    :accessor job-run-token
    :type (or null string)
    :documentation "The token keeping a delayed interrupt off a later job.")
   (result
    :initform nil
    :accessor job-result
    :documentation "The value the job body returned, or NIL when it did not.")
   (condition-report
    :initform nil
    :accessor job-condition-report
    :type (or null string)
    :documentation "The bounded report of the condition that ended the job.")
   (cancellation-reason
    :initform nil
    :accessor job-cancellation-reason
    :type (or null keyword)
    :documentation "The structured reason a controller requested cancellation.")
   (retained-p
    :initform nil
    :accessor job--retained-p
    :type boolean
    :documentation "True once terminal retention has accounted for this job.")
   (progress
    :initform (make-instance 'job-progress)
    :reader job--progress
    :type job-progress
    :documentation "The normalized progress record of this job.")
   (created-at
    :initform (get-internal-real-time)
    :reader job-created-at
    :type integer
    :documentation "The internal real time at which the pool admitted the job.")
   (started-at
    :initform nil
    :accessor job-started-at
    :documentation "The internal real time execution began, or NIL.")
   (deadline
    :initform nil
    :accessor job-deadline
    :type (or null integer)
    :documentation "The internal real time the monitor cancels this job.")
   (ended-at
    :initform nil
    :accessor job-ended-at
    :documentation "The internal real time of terminal publication, or NIL."))
  (:documentation "One supervised execution of a caller-supplied job body."))


;;;; -- Narrow Adapters --

(defun jobpond--condition-broadcast (condition-variable)
  "Wake every waiter on CONDITION-VARIABLE and return NIL.

Bordeaux threads only promises to wake one waiter, so this narrow adapter uses
the host broadcast primitive where one exists. Pool waiters therefore never
depend on a single notification reaching the one waiter that can make progress."
  #+sbcl
  (sb-thread:condition-broadcast condition-variable)
  #-sbcl
  (condition-notify condition-variable)
  nil)

(defun jobpond--milliseconds-between (start end)
  "Return the elapsed milliseconds between internal real times START and END."
  (round (* 1000 (- end start)) internal-time-units-per-second))

(defun jobpond--bounded-string (text &key (limit *retained-report-limit*))
  "Return at most LIMIT leading characters of TEXT."
  (if (<= (length text) limit)
      text
      (subseq text 0 limit)))

(defun jobpond--identifier-fragment (value)
  "Return VALUE as a bounded lowercase identifier fragment, or NIL.

Characters outside the alphanumeric, hyphen, and underscore set become hyphens,
so a caller-supplied name never makes an identifier awkward to read or type."
  (if (null value)
      nil
      (let* ((trimmed (string-trim '(#\Space #\Tab #\Newline #\Return #\Page)
                                   value))
             (unbounded (string-downcase trimmed))
             (text (subseq unbounded
                           0
                           (min (length unbounded) *identifier-fragment-limit*)))
             (mapped (map 'string
                          (lambda (character)
                            (if (or (alphanumericp character)
                                    (char= character #\-)
                                    (char= character #\_))
                                character
                                #\-))
                          text))
             (fragment (string-trim '(#\-) mapped)))
        (if (plusp (length fragment))
            fragment
            nil))))


;;;; -- Job State Predicates --

(defun job--terminal-state-p (state)
  "Return T when STATE is a published terminal job state, else NIL."
  (if (member state '(:completed :failed :aborted) :test #'eq)
      t
      nil))

(defun job--next-run-token ()
  "Return a fresh run token that no other job execution can carry.

Cancellation interrupts compare this token against the token bound on the
interrupted worker, so uniqueness across the whole image is what keeps a
delayed interrupt from striking whichever job that worker picked up next."
  (with-lock-held (*run-token-lock*)
    (incf *run-token-index*)
    (format nil "job-run-~D" *run-token-index*)))


;;;; -- Limit Validation --

(defun job-pool--validate-limit (limit-kind value &key (minimum 1) maximum)
  "Return VALUE when it is an integer within range, else signal an error.

Signals JOB-POOL-INVALID-LIMIT naming LIMIT-KIND so a caller can report which
pool bound it supplied badly."
  (if (and (integerp value)
           (>= value minimum)
           (or (null maximum) (<= value maximum)))
      value
      (error 'job-pool-invalid-limit
             :message
             (if maximum
                 (format nil
                         "The ~(~A~) limit must be an integer from ~D to ~D, not ~S."
                         limit-kind minimum maximum value)
                 (format nil
                         "The ~(~A~) limit must be at least ~D, not ~S."
                         limit-kind minimum value))
             :limit-kind limit-kind
             :value value)))


;;;; -- Listeners and Events --

(defun job-pool-add-listener (pool listener)
  "Register LISTENER for POOL's portable events and return it.

LISTENER is called with a channel keyword and an event plist. It runs on the
thread that produced the event, so it must not block or signal."
  (check-type listener function)
  (with-lock-held ((job-pool--lock pool))
    (pushnew listener (job-pool--listeners pool) :test #'eq))
  listener)

(defun job-pool-remove-listener (pool listener)
  "Remove LISTENER from POOL and return NIL."
  (with-lock-held ((job-pool--lock pool))
    (setf (job-pool--listeners pool)
          (remove listener (job-pool--listeners pool) :test #'eq)))
  nil)

(defun job-pool-emit (pool channel payload)
  "Deliver CHANNEL and PAYLOAD to a snapshot of POOL's listeners.

A listener that signals is ignored, so an observer can never turn a job into a
pool failure. The listener list is copied under the pool lock, so a listener may
add or remove listeners without deadlocking."
  (let ((listeners
          (with-lock-held ((job-pool--lock pool))
            (copy-list (job-pool--listeners pool)))))
    (dolist (listener listeners)
      (handler-case
          (funcall listener channel payload)
        (serious-condition ()
          nil))))
  nil)


;;;; -- Pool Inspection --

(defun job-pool-live-count (pool)
  "Return POOL's admitted queued, running, and publishing job count."
  (with-lock-held ((job-pool--lock pool))
    (job-pool--live-count pool)))

(defun job-pool-active-count (pool)
  "Return the jobs POOL is currently running on reusable workers."
  (with-lock-held ((job-pool--lock pool))
    (job-pool--active-count pool)))

(defun job-pool-queued-count (pool)
  "Return the jobs POOL has admitted but not yet started."
  (with-lock-held ((job-pool--lock pool))
    (length (job-pool--queue pool))))

(defun job-pool--collect-jobs-locked (pool)
  "Return every live and retained job of POOL while its lock is held."
  (loop for job being the hash-values of (job-pool--jobs pool)
        collect job))

(defun job-pool--collect-threads-locked (pool)
  "Return POOL's monitor and reusable worker threads while its lock is held."
  (remove nil
          (cons (job-pool--monitor-thread pool)
                (copy-list (job-pool--worker-threads pool)))))

(defun job-pool-list-jobs (pool)
  "Return POOL's live and retained jobs ordered by admission index."
  (let ((jobs (with-lock-held ((job-pool--lock pool))
                (job-pool--collect-jobs-locked pool))))
    (sort jobs #'< :key #'job-index)))

(defun job-pool-find-job (pool identifier)
  "Return POOL's job named IDENTIFIER or signal JOB-NOT-FOUND.

A job disappears once terminal retention evicts it, so a caller holding an old
identifier must be ready for this failure."
  (let ((job (with-lock-held ((job-pool--lock pool))
               (gethash identifier (job-pool--jobs pool)))))
    (if job
        job
        (error 'job-not-found
               :message (format nil "No job named ~A exists in pool ~A."
                                identifier (job-pool-name pool))
               :identifier identifier))))

(defun job-pool-live-jobs (pool)
  "Return POOL's queued and running jobs ordered by admission index."
  (remove-if-not
   (lambda (job)
     (member (job-state job) '(:queued :running) :test #'eq))
   (job-pool-list-jobs pool)))
