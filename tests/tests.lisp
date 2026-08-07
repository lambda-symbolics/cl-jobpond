(in-package #:cl-jobpond/tests)

;;;; -- Test Support --

(define-condition tests--rude-condition (serious-condition)
  nil
  (:report
   (lambda (condition stream)
     (declare (ignore condition))
     (write-string "a rude serious condition" stream)))
  (:documentation
   "A serious condition that is neither an ERROR nor a JOB-ABORTED.

A job body signalling this escapes the execution handlers and must be caught by
the worker-loop backstop instead."))

(defclass tests--gate ()
  ((lock
    :initform (make-lock "cl-jobpond test gate")
    :reader tests--gate-lock
    :documentation "The lock protecting the open flag.")
   (condition-variable
    :initform (make-condition-variable)
    :reader tests--gate-condition-variable
    :documentation "The condition woken when the gate opens.")
   (open-p
    :initform nil
    :accessor tests--gate-open-p
    :type boolean
    :documentation "True once the gate has been opened."))
  (:documentation
   "A one-shot gate letting a test release a blocked job body deterministically."))

(defun tests--make-gate ()
  "Return a fresh closed gate."
  (make-instance 'tests--gate))

(defun tests--gate-open (gate)
  "Open GATE, wake its waiters, and return NIL."
  (with-lock-held ((tests--gate-lock gate))
    (setf (tests--gate-open-p gate) t)
    (condition-notify (tests--gate-condition-variable gate)))
  nil)

(defun tests--gate-await (gate &key (timeout-seconds 30))
  "Wait for GATE to open and return T when it did, or NIL after the timeout.

The wait rechecks the flag on a short condition timeout, so a missed
notification only costs latency and never hangs a test."
  (let ((deadline (+ (get-internal-real-time)
                     (* timeout-seconds internal-time-units-per-second))))
    (with-lock-held ((tests--gate-lock gate))
      (loop
        (when (tests--gate-open-p gate)
          (return t))
        (when (>= (get-internal-real-time) deadline)
          (return nil))
        (condition-wait (tests--gate-condition-variable gate)
                        (tests--gate-lock gate)
                        :timeout 0.02)))))

(defun tests--wait-until (predicate &key (timeout-seconds 30))
  "Return T once PREDICATE returns true, or NIL once TIMEOUT-SECONDS pass."
  (let ((deadline (+ (get-internal-real-time)
                     (* timeout-seconds internal-time-units-per-second))))
    (loop
      (when (funcall predicate)
        (return t))
      (when (>= (get-internal-real-time) deadline)
        (return nil))
      (sleep 0.002))))

(defclass tests--counter ()
  ((lock
    :initform (make-lock "cl-jobpond test counter")
    :reader tests--counter-lock
    :documentation "The lock protecting the counter fields.")
   (value
    :initform 0
    :accessor tests--counter-value
    :type integer
    :documentation "The number of job bodies currently inside the counter.")
   (maximum
    :initform 0
    :accessor tests--counter-maximum-value
    :type integer
    :documentation "The largest simultaneous occupancy observed so far."))
  (:documentation "A thread-safe occupancy counter observing real concurrency."))

(defun tests--counter-enter (counter)
  "Record entry into COUNTER and return the new occupancy."
  (with-lock-held ((tests--counter-lock counter))
    (incf (tests--counter-value counter))
    (setf (tests--counter-maximum-value counter)
          (max (tests--counter-maximum-value counter)
               (tests--counter-value counter)))))

(defun tests--counter-leave (counter)
  "Record departure from COUNTER and return the new occupancy."
  (with-lock-held ((tests--counter-lock counter))
    (decf (tests--counter-value counter))))

(defun tests--counter-maximum (counter)
  "Return the largest simultaneous occupancy COUNTER has observed."
  (with-lock-held ((tests--counter-lock counter))
    (tests--counter-maximum-value counter)))

(defclass tests--collector ()
  ((lock
    :initform (make-lock "cl-jobpond test collector")
    :reader tests--collector-lock
    :documentation "The lock protecting the recorded events.")
   (events
    :initform nil
    :accessor tests--collector-recorded
    :type list
    :documentation "The recorded events, newest first."))
  (:documentation "A thread-safe sink recording pool listener events."))

(defun tests--make-collector ()
  "Return a fresh empty event collector."
  (make-instance 'tests--collector))

(defun tests--collector-listener (collector)
  "Return a pool listener recording every event into COLLECTOR."
  (lambda (channel payload)
    (with-lock-held ((tests--collector-lock collector))
      (push (cons channel payload) (tests--collector-recorded collector)))
    nil))

(defun tests--collector-events (collector &key channel)
  "Return COLLECTOR's events oldest first, optionally limited to CHANNEL."
  (let ((events (with-lock-held ((tests--collector-lock collector))
                  (reverse (copy-list (tests--collector-recorded collector))))))
    (if channel
        (remove-if-not (lambda (event) (eq (first event) channel)) events)
        events)))

(defun tests--statuses (collector)
  "Return the lifecycle statuses COLLECTOR recorded, oldest first."
  (mapcar (lambda (event) (getf (rest event) :status))
          (tests--collector-events collector :channel :job-lifecycle)))

(defmacro with-test-pool ((variable &rest options) &body body)
  "Bind VARIABLE to a fresh pool built from OPTIONS and close it afterwards.

The normal exit asserts that the pool closed completely, so a leaked worker or
monitor thread fails the test that produced it instead of the next one. An
abnormal exit still closes the pool, but leaves the original failure visible."
  (let ((closed-p (gensym "CLOSED-P")))
    `(let ((,variable (make-job-pool ,@options))
           (,closed-p nil))
       (unwind-protect
            (multiple-value-prog1
                (progn ,@body)
              (setf ,closed-p (job-pool-close ,variable))
              (test-assert ,closed-p
                           (format nil "pool ~A closes completely"
                                   (job-pool-name ,variable))))
         (unless ,closed-p
           (job-pool-close ,variable))))))

(defun tests--await-completed (job)
  "Await JOB with a generous timeout and return its snapshot."
  (multiple-value-bind (snapshot terminal-p)
      (job-await job :timeout-seconds 60)
    (test-assert terminal-p
                 (format nil "job ~A reaches a terminal state"
                         (job-identifier job)))
    snapshot))


;;;; -- Execution and Result Typing --

(defparameter *tests--execution-cases*
  (list (list :name "value"
              :payload 21
              :body (lambda (job) (* 2 (job-payload job)))
              :state :completed
              :result 42
              :report-p nil)
        (list :name "empty"
              :payload nil
              :body (lambda (job) (declare (ignore job)) nil)
              :state :completed
              :result nil
              :report-p nil)
        (list :name "error"
              :payload nil
              :body (lambda (job)
                      (declare (ignore job))
                      (error "a deliberate job failure"))
              :state :failed
              :result nil
              :report-p t)
        (list :name "abort"
              :payload nil
              :body (lambda (job)
                      (error 'job-aborted
                             :message "the body aborted itself"
                             :identifier (job-identifier job)
                             :reason :self))
              :state :aborted
              :result nil
              :report-p t)
        (list :name "rude"
              :payload nil
              :body (lambda (job)
                      (declare (ignore job))
                      (error 'tests--rude-condition))
              :state :failed
              :result nil
              :report-p t)
        (list :name "after-rude"
              :payload 5
              :body (lambda (job) (1+ (job-payload job)))
              :state :completed
              :result 6
              :report-p nil))
  "Job bodies paired with the terminal state and result each must produce.")

(defun tests--execution ()
  "Exercise every terminal outcome a job body can produce."
  (with-test-pool (pool :name "cl-jobpond test execution" :maximum-concurrency 1)
    (dolist (case *tests--execution-cases*)
      (let* ((label (getf case :name))
             (job (job-pool-submit pool
                                   (getf case :body)
                                   :name label
                                   :payload (getf case :payload)))
             (snapshot (tests--await-completed job)))
        (test-assert (eq (getf snapshot :state) (getf case :state))
                     (format nil "the ~A body reaches ~A"
                             label (getf case :state)))
        (test-assert (equal (getf snapshot :result) (getf case :result))
                     (format nil "the ~A body publishes its result" label))
        (test-assert (eq (and (getf snapshot :condition-report) t)
                         (getf case :report-p))
                     (format nil "the ~A body reports its condition" label))
        (test-assert (job-terminal-p job)
                     (format nil "the ~A job is terminal" label))
        (test-assert (equal (job-name job) label)
                     (format nil "the ~A job keeps its name" label))
        (test-assert (null (job-deadline job))
                     (format nil "the ~A job releases its deadline" label))
        (test-assert (null (job-run-token job))
                     (format nil "the ~A job releases its run token" label))))
    (test-assert (tests--wait-until
                  (lambda () (zerop (job-pool-active-count pool))))
                 "every worker returns to idle after a failed body")
    (test-assert (zerop (job-pool-live-count pool))
                 "no job stays live after the whole table has run"))
  nil)

(defparameter *tests--identifier-cases*
  '(("Hello World" . "hello-world-1")
    (nil . "job-2")
    ("  spaced  " . "spaced-3")
    ("***" . "job-4")
    ("MiXeD_case-9" . "mixed_case-9-5"))
  "Submitted job names paired with the identifier each must receive.")

(defun tests--identifiers ()
  "Exercise identifier fragments derived from caller-supplied job names."
  (with-test-pool (pool :name "cl-jobpond test identifiers"
                        :maximum-concurrency 2)
    (let ((jobs (job-pool-submit-batch
                 pool
                 (mapcar (lambda (case)
                           (list :function (lambda (job) (job-identifier job))
                                 :name (first case)))
                         *tests--identifier-cases*))))
      (test-assert (= (length jobs) (length *tests--identifier-cases*))
                   "a batch returns one job per entry")
      (loop for job in jobs
            for case in *tests--identifier-cases*
            for expected = (rest case)
            for index from 1
            do (test-assert (string= (job-identifier job) expected)
                            (format nil "the name ~S yields identifier ~A"
                                    (first case) expected))
               (test-assert (= (job-index job) index)
                            (format nil "job ~A carries admission index ~D"
                                    expected index))
               (test-assert (string= (getf (tests--await-completed job) :result)
                                     expected)
                            (format nil "job ~A sees its own identifier"
                                    expected)))))
  nil)

(defun tests--concurrency-bound ()
  "Exercise the hard bound on jobs running at the same time."
  (let ((counter (make-instance 'tests--counter)))
    (with-test-pool (pool :name "cl-jobpond test concurrency"
                          :maximum-concurrency 2)
      (let ((jobs (loop repeat 6
                        collect
                        (job-pool-submit
                         pool
                         (lambda (job)
                           (declare (ignore job))
                           (tests--counter-enter counter)
                           (tests--wait-until
                            (lambda ()
                              (>= (tests--counter-maximum counter) 2))
                            :timeout-seconds 20)
                           (tests--counter-leave counter)
                           :ran)
                         :name "bounded"))))
        (dolist (job jobs)
          (test-assert (eq (getf (tests--await-completed job) :result) :ran)
                       "every bounded job completes"))
        (test-assert (= (tests--counter-maximum counter) 2)
                     "exactly the concurrency bound runs at the same time"))))
  nil)


;;;; -- Admission Bounds --

(defun tests--admission-bounds ()
  "Exercise the batch-size, live-job, and entry validity refusals."
  (let ((release (tests--make-gate))
        (started (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test bounds"
                          :maximum-concurrency 1
                          :maximum-batch-size 2
                          :maximum-live-jobs 2)
      (let ((blocker (job-pool-submit
                      pool
                      (lambda (job)
                        (declare (ignore job))
                        (tests--gate-open started)
                        (tests--gate-await release))
                      :name "blocker"))
            (queued (job-pool-submit pool
                                     (lambda (job) (job-identifier job))
                                     :name "queued")))
        (test-assert (tests--gate-await started)
                     "the first job starts on the only worker")
        (test-assert (tests--wait-until
                      (lambda () (= (job-pool-queued-count pool) 1)))
                     "the second job waits in the queue")
        (test-assert (= (job-pool-live-count pool) 2)
                     "both admitted jobs count as live")
        (test-assert (= (job-pool-active-count pool) 1)
                     "only one admitted job is running")
        (test-assert (= (length (job-pool-live-jobs pool)) 2)
                     "both admitted jobs are reported live")
        (let ((refusal
                (handler-case
                    (progn (job-pool-submit pool
                                            (lambda (job) job)
                                            :name "refused")
                           nil)
                  (job-pool-capacity-exceeded (condition) condition))))
          (test-assert refusal
                       "a third job exceeds the live-job bound")
          (test-assert (eq (job-pool-capacity-exceeded-limit-kind refusal)
                           :live-jobs)
                       "the refusal names the live-job bound")
          (test-assert (= (job-pool-capacity-exceeded-limit refusal) 2)
                       "the refusal reports the bound in force")
          (test-assert (= (job-pool-capacity-exceeded-requested-count refusal) 1)
                       "the refusal reports the requested job count")
          (test-assert (= (job-pool-capacity-exceeded-live-count refusal) 2)
                       "the refusal reports the live job count"))
        (let ((refusal
                (handler-case
                    (progn (job-pool-submit-batch
                            pool
                            (list (list :function (lambda (job) job))
                                  (list :function (lambda (job) job))
                                  (list :function (lambda (job) job))))
                           nil)
                  (job-pool-capacity-exceeded (condition) condition))))
          (test-assert (and refusal
                            (eq (job-pool-capacity-exceeded-limit-kind refusal)
                                :batch-size))
                       "an oversized batch exceeds the batch-size bound"))
        (test-assert (signals job-pool-invalid-entry
                       (job-pool-submit-batch pool (list (list :name "no body"))))
                     "an entry without a body is refused")
        (test-assert (signals job-pool-invalid-entry
                       (job-pool-submit-batch
                        pool
                        (list (list :function (lambda (job) job)
                                    :name :not-a-string))))
                     "an entry with a non-string name is refused")
        (test-assert (signals job-pool-invalid-limit
                       (job-pool-submit pool
                                        (lambda (job) job)
                                        :maximum-runtime-milliseconds -1))
                     "an entry with a negative runtime cap is refused")
        (test-assert (= (job-pool-live-count pool) 2)
                     "every refused admission leaves the pool untouched")
        (test-assert (null (job-pool-submit-batch pool nil))
                     "an empty batch admits nothing")
        (tests--gate-open release)
        (test-assert (tests--await-completed blocker)
                     "the blocking job finishes once released")
        (test-assert (string= (getf (tests--await-completed queued) :result)
                              (job-identifier queued))
                     "the queued job runs after the worker frees up")
        (test-assert (tests--wait-until
                      (lambda () (zerop (job-pool-live-count pool))))
                     "the live count returns to zero"))))
  nil)

(defparameter *tests--invalid-limit-cases*
  '((:maximum-concurrency 0)
    (:maximum-concurrency 33)
    (:maximum-concurrency :eight)
    (:maximum-batch-size 0)
    (:maximum-live-jobs 0)
    (:maximum-runtime-milliseconds -1)
    (:terminal-retention-limit 0))
  "Pool limit keywords paired with a value the constructor must refuse.")

(defun tests--invalid-limits ()
  "Exercise pool construction refusing unusable limits."
  (dolist (case *tests--invalid-limit-cases*)
    (let ((refusal
            (handler-case
                (progn (apply #'make-job-pool
                              :name "cl-jobpond test invalid"
                              case)
                       nil)
              (job-pool-invalid-limit (condition) condition))))
      (test-assert refusal
                   (format nil "~S is refused for ~S"
                           (second case) (first case)))
      (test-assert (eq (job-pool-invalid-limit-kind refusal) (first case))
                   (format nil "the refusal of ~S names its limit"
                           (first case)))))
  nil)


;;;; -- Cancellation --

(defparameter *tests--interrupt-guard-cases*
  '((:current :first :publishing nil :bound "token-1" :token "token-1" :expect t)
    (:current :second :publishing nil :bound "token-1" :token "token-1"
     :expect nil)
    (:current :first :publishing :first :bound "token-1" :token "token-1"
     :expect nil)
    (:current :first :publishing nil :bound "token-2" :token "token-1"
     :expect nil)
    (:current :first :publishing nil :bound nil :token "token-1" :expect nil)
    (:current nil :publishing nil :bound "token-1" :token "token-1" :expect nil))
  "Worker states paired with whether a cancellation interrupt may fire in them.")

(defun tests--interrupt-guard ()
  "Exercise the guard deciding whether a delivered interrupt still applies.

The guard is what keeps a cancellation aimed at one job from unwinding whichever
job its worker picked up next, so every state it distinguishes is checked here."
  (with-test-pool (pool :name "cl-jobpond test guard" :maximum-concurrency 1)
    (let* ((first-job (job-pool-submit pool
                                       (lambda (job) (job-identifier job))
                                       :name "first"))
           (second-job (job-pool-submit pool
                                        (lambda (job) (job-identifier job))
                                        :name "second")))
      (tests--await-completed first-job)
      (tests--await-completed second-job)
      (dolist (case *tests--interrupt-guard-cases*)
        (let* ((jobs (list :first first-job :second second-job))
               (cl-jobpond::*current-job* (getf jobs (getf case :current)))
               (cl-jobpond::*terminal-publication-job*
                 (getf jobs (getf case :publishing)))
               (cl-jobpond::*current-run-token* (getf case :bound))
               (applicable-p
                 (cl-jobpond::job--cancellation-interrupt-applicable-p
                  first-job
                  (getf case :token))))
          (test-assert (eq applicable-p (getf case :expect))
                       (format nil
                               "an interrupt for the first job with current ~S, ~
publishing ~S, and bound token ~S is ~:[declined~;applied~]"
                               (getf case :current)
                               (getf case :publishing)
                               (getf case :bound)
                               (getf case :expect)))))))
  nil)

(defun tests--running-cancellation ()
  "Exercise cancelling a running job and reusing its worker afterwards."
  (let ((started (tests--make-gate))
        (never (tests--make-gate))
        (cleanup-ran-p nil))
    (with-test-pool (pool :name "cl-jobpond test cancel" :maximum-concurrency 1)
      (let ((job (job-pool-submit
                  pool
                  (lambda (job)
                    (declare (ignore job))
                    (unwind-protect
                         (progn (tests--gate-open started)
                                (tests--gate-await never))
                      (setf cleanup-ran-p t)))
                  :name "victim")))
        (test-assert (tests--gate-await started)
                     "the cancelled job reaches its body")
        (test-assert (job-cancel job)
                     "the first cancellation request is accepted")
        (test-assert (null (job-cancel job :reason :shutdown))
                     "a second cancellation request is declined")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "an interrupted job publishes as aborted")
          (test-assert (eq (getf snapshot :cancellation-reason) :cancelled)
                       "the published job keeps the first cancellation reason")
          (test-assert (null (getf snapshot :result))
                       "an interrupted job publishes no result")
          (test-assert (search "cancelled" (getf snapshot :condition-report))
                       "the interrupted job reports its cancellation"))
        (test-assert cleanup-ran-p
                     "the interrupted body runs its cleanup forms")
        (test-assert (eq (job-cancellation-reason job) :cancelled)
                     "the cancellation reason survives publication"))
      (dolist (label '("after-one" "after-two" "after-three"))
        (let ((later (job-pool-submit pool
                                      (lambda (job) (job-identifier job))
                                      :name label)))
          (test-assert (eq (getf (tests--await-completed later) :state)
                           :completed)
                       (format nil "job ~A survives the earlier interrupt"
                               label))))))
  nil)

(defun tests--queued-cancellation ()
  "Exercise cancelling a job that no worker has started yet."
  (let ((release (tests--make-gate))
        (started (tests--make-gate))
        (body-ran-p nil))
    (with-test-pool (pool :name "cl-jobpond test queued cancel"
                          :maximum-concurrency 1)
      (let ((blocker (job-pool-submit
                      pool
                      (lambda (job)
                        (declare (ignore job))
                        (tests--gate-open started)
                        (tests--gate-await release))
                      :name "blocker"))
            (queued (job-pool-submit pool
                                     (lambda (job)
                                       (declare (ignore job))
                                       (setf body-ran-p t))
                                     :name "queued")))
        (test-assert (tests--gate-await started)
                     "the blocking job occupies the only worker")
        (test-assert (tests--wait-until
                      (lambda () (= (job-pool-queued-count pool) 1)))
                     "the second job is queued before cancellation")
        (test-assert (job-cancel queued :reason :discarded)
                     "a queued job accepts cancellation")
        (let ((snapshot (tests--await-completed queued)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "a queued job publishes as aborted without running")
          (test-assert (search "before it started"
                               (getf snapshot :condition-report))
                       "the queued job reports that it never started"))
        (test-assert (zerop (job-pool-queued-count pool))
                     "a cancelled job leaves the queue")
        (test-assert (null (cl-jobpond::job--publish-terminal
                            queued :completed :late))
                     "a late writer is told it did not publish")
        (test-assert (eq (job-state queued) :aborted)
                     "a late writer cannot change a published state")
        (test-assert (null (job-result queued))
                     "a late writer cannot install a second result")
        (tests--gate-open release)
        (tests--await-completed blocker)
        (test-assert (null body-ran-p)
                     "the cancelled queued body never ran"))))
  nil)

(defun tests--cooperative-cancellation ()
  "Exercise a body that stops at its own cancellation checks."
  (let ((started (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test cooperative"
                          :maximum-concurrency 1)
      (let ((job (job-pool-submit
                  pool
                  (lambda (job)
                    (handler-case
                        (loop
                          (tests--gate-open started)
                          (job-report-progress job :detail :working)
                          (job-check-cancellation job)
                          (sleep 0.002))
                      (error ()
                        :swallowed)))
                  :name "cooperative")))
        (test-assert (tests--gate-await started)
                     "the cooperative job reaches its loop")
        (test-assert (job-cancel job :reason :superseded)
                     "the cooperative job accepts cancellation")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "a cooperative cancellation still publishes as aborted")
          (test-assert (eq (getf snapshot :cancellation-reason) :superseded)
                       "the cooperative job keeps its structured reason")
          (test-assert (null (getf snapshot :result))
                       "a body handling ERROR cannot swallow its cancellation")))))
  nil)

(defun tests--late-completion-downgrade ()
  "Exercise a cancelled body whose own handler lets it return normally."
  (let ((entered (tests--make-gate))
        (never (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test downgrade"
                          :maximum-concurrency 1)
      (let ((job (job-pool-submit
                  pool
                  (lambda (job)
                    (declare (ignore job))
                    (handler-case
                        (progn (tests--gate-open entered)
                               (tests--gate-await never))
                      (job-aborted ()
                        nil))
                    :finished-anyway)
                  :name "downgraded")))
        (test-assert (tests--gate-await entered)
                     "the downgraded body reaches its own abort handler")
        (test-assert (job-cancel job :reason :timeout)
                     "the running job accepts cancellation")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "a cancelled job cannot publish success")
          (test-assert (null (getf snapshot :result))
                       "the discarded result never reaches the snapshot")
          (test-assert (eq (getf snapshot :cancellation-reason) :timeout)
                       "the downgraded publication keeps the reason")))))
  nil)


;;;; -- Deadlines --

(defun tests--pool-deadline ()
  "Exercise the pool-wide wall-clock cap cancelling an overrunning job."
  (let ((never (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test pool deadline"
                          :maximum-concurrency 1
                          :maximum-runtime-milliseconds 50)
      (let ((job (job-pool-submit pool
                                  (lambda (job)
                                    (declare (ignore job))
                                    (tests--gate-await never))
                                  :name "overrun")))
        (test-assert (= (job-maximum-runtime-milliseconds job) 50)
                     "an admitted job inherits the pool wall-clock cap")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "an overrunning job is aborted by the monitor")
          (test-assert (eq (getf snapshot :cancellation-reason) :timeout)
                       "the monitor records a timeout reason")
          (test-assert (null (job-deadline job))
                       "a published job releases its deadline")))))
  nil)

(defun tests--job-deadline ()
  "Exercise a per-job wall-clock cap in a pool that has none of its own."
  (let ((never (tests--make-gate))
        (unbounded (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test job deadline"
                          :maximum-concurrency 2
                          :maximum-runtime-milliseconds 0)
      (let ((bounded (job-pool-submit pool
                                      (lambda (job)
                                        (declare (ignore job))
                                        (tests--gate-await never))
                                      :name "bounded"
                                      :maximum-runtime-milliseconds 50))
            (patient (job-pool-submit pool
                                      (lambda (job)
                                        (declare (ignore job))
                                        (tests--gate-await unbounded
                                                           :timeout-seconds 0.5)
                                        :survived)
                                      :name "patient")))
        (test-assert (zerop (job-maximum-runtime-milliseconds patient))
                     "a job without a cap inherits the disabled pool cap")
        (let ((snapshot (tests--await-completed bounded)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "a job with its own cap is aborted on expiry")
          (test-assert (eq (getf snapshot :cancellation-reason) :timeout)
                       "the per-job expiry records a timeout reason"))
        (let ((snapshot (tests--await-completed patient)))
          (test-assert (eq (getf snapshot :state) :completed)
                       "a job without a cap outlives its neighbour's deadline")
          (test-assert (eq (getf snapshot :result) :survived)
                       "the uncapped job publishes its own result")))))
  nil)

(defun tests--await-timeout ()
  "Exercise a bounded wait returning before the job is terminal."
  (let ((release (tests--make-gate))
        (started (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test await"
                          :maximum-concurrency 1)
      (let ((job (job-pool-submit pool
                                  (lambda (job)
                                    (declare (ignore job))
                                    (tests--gate-open started)
                                    (tests--gate-await release)
                                    :released)
                                  :name "slow")))
        (test-assert (tests--gate-await started)
                     "the slow job starts before the bounded wait")
        (multiple-value-bind (snapshot terminal-p)
            (job-await job :timeout-seconds 0.05)
          (test-assert (null terminal-p)
                       "a bounded wait reports that the job is not terminal")
          (test-assert (eq (getf snapshot :state) :running)
                       "the timed-out wait still returns a coherent snapshot")
          (test-assert (integerp (getf (getf snapshot :progress)
                                       :duration-milliseconds))
                       "a running snapshot times itself against the clock"))
        (tests--gate-open release)
        (test-assert (eq (getf (tests--await-completed job) :result) :released)
                     "the released job publishes its result"))))
  nil)


;;;; -- Progress --

(defun tests--progress ()
  "Exercise progress reporting, bounded output, and terminal compaction."
  (let ((reported (tests--make-gate))
        (release (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test progress"
                          :maximum-concurrency 1)
      (let ((job (job-pool-submit
                  pool
                  (lambda (job)
                    (job-report-progress job
                                         :detail :first-step
                                         :output (make-string 20000
                                                              :initial-element
                                                              #\x))
                    (job-report-progress job :detail :second-step :steps 2)
                    (tests--gate-open reported)
                    (tests--gate-await release)
                    :done)
                  :name "reporter")))
        (test-assert (tests--gate-await reported)
                     "the reporting job reaches its gate")
        (let ((progress (job-progress-snapshot job)))
          (test-assert (eq (getf progress :status) :running)
                       "a live progress snapshot reports the running status")
          (test-assert (eq (getf progress :detail) :second-step)
                       "the newest reported detail wins")
          (test-assert (= (getf progress :step-count) 3)
                       "reported steps accumulate")
          (test-assert (= (length (getf progress :recent-output))
                          *progress-output-limit*)
                       "streamed output keeps only a bounded live tail")
          (test-assert (string= (getf progress :identifier) (job-identifier job))
                       "a progress snapshot names its own job"))
        (tests--gate-open release)
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf (getf snapshot :progress) :status) :completed)
                       "terminal publication mirrors the state into progress")
          (test-assert (= (length (getf (getf snapshot :progress)
                                        :recent-output))
                          *retained-progress-output-limit*)
                       "a terminal job compacts its retained output tail")
          (test-assert (integerp (getf snapshot :started-at))
                       "a completed job records when it started")
          (test-assert (integerp (getf snapshot :ended-at))
                       "a completed job records when it ended")
          (test-assert (integerp (getf (getf snapshot :progress)
                                       :duration-milliseconds))
                       "a completed job reports a duration"))
        (test-assert (null (job-cancellation-requested-p job))
                     "an uncancelled job reports no cancellation request")
        (test-assert (null (job-check-cancellation job))
                     "checking an uncancelled job signals nothing"))))
  nil)


;;;; -- Listeners --

(defun tests--listeners ()
  "Exercise lifecycle and progress listeners, including a failing listener."
  (let ((collector (tests--make-collector))
        (rude-calls 0))
    (with-test-pool (pool :name "cl-jobpond test listeners"
                          :maximum-concurrency 1)
      (let ((rude (lambda (channel payload)
                    (declare (ignore channel payload))
                    (incf rude-calls)
                    (error "a listener that misbehaves")))
            (listener (tests--collector-listener collector)))
        (job-pool-add-listener pool rude)
        (test-assert (eq (job-pool-add-listener pool listener) listener)
                     "adding a listener returns it")
        (let ((job (job-pool-submit
                    pool
                    (lambda (job)
                      (job-report-progress job :detail :halfway)
                      :listened)
                    :name "watched")))
          (tests--await-completed job)
          (test-assert (tests--wait-until
                        (lambda ()
                          (equal (tests--statuses collector)
                                 '(:started :completed))))
                       "a job emits a started and a completed lifecycle event")
          (let ((progress (tests--collector-events collector
                                                   :channel :job-progress)))
            (test-assert (= (length progress) 1)
                         "one progress report emits one progress event")
            (test-assert (eq (getf (rest (first progress)) :detail) :halfway)
                         "a progress event carries the reported detail")
            (test-assert (string= (getf (rest (first progress)) :identifier)
                                  (job-identifier job))
                         "a progress event names its own job")
            (test-assert (= (getf (rest (first progress)) :step-count) 1)
                         "a progress event carries the step count"))
          (test-assert (plusp rude-calls)
                       "a failing listener is still called")
          (test-assert (eq (getf (job-snapshot job) :state) :completed)
                       "a failing listener cannot fail its job"))
        (job-pool-emit pool :job-custom '(:identifier "manual"))
        (test-assert (tests--wait-until
                      (lambda ()
                        (= (length (tests--collector-events
                                    collector :channel :job-custom))
                           1)))
                     "a pool delivers a directly emitted event")
        (job-pool-remove-listener pool listener)
        (let ((before (length (tests--collector-events collector))))
          (tests--await-completed
           (job-pool-submit pool (lambda (job) (job-identifier job))
                            :name "unwatched"))
          (job-pool-emit pool :job-custom '(:identifier "after"))
          (test-assert (= (length (tests--collector-events collector)) before)
                       "a removed listener receives no further events")))))
  nil)


;;;; -- Retention --

(defun tests--retention-ring ()
  "Exercise the bounded retention ring evicting the oldest terminal jobs."
  (with-test-pool (pool :name "cl-jobpond test retention"
                        :maximum-concurrency 1
                        :terminal-retention-limit 3)
    (let ((jobs (loop repeat 6
                      collect
                      (let ((job (job-pool-submit
                                  pool
                                  (lambda (job) (job-identifier job))
                                  :name "ring")))
                        (tests--await-completed job)
                        job))))
      (test-assert (zerop (job-pool-live-count pool))
                   "every finished job leaves the live count")
      (let ((retained (job-pool-list-jobs pool)))
        (test-assert (= (length retained) 3)
                     "the retention ring keeps only its limit")
        (test-assert (equal (mapcar #'job-index retained) '(4 5 6))
                     "the ring keeps the newest terminal jobs"))
      (dolist (job (subseq jobs 3))
        (test-assert (eq (job-pool-find-job pool (job-identifier job)) job)
                     (format nil "job ~A is still retained"
                             (job-identifier job))))
      (let* ((evicted (job-identifier (first jobs)))
             (refusal (handler-case
                          (progn (job-pool-find-job pool evicted) nil)
                        (job-not-found (condition) condition))))
        (test-assert refusal
                     "an evicted job can no longer be found")
        (test-assert (string= (job-not-found-identifier refusal) evicted)
                     "the lookup failure names the missing identifier"))
      (test-assert (getf (job-snapshot (first jobs)) :result)
                   "an evicted job object still holds its own result")))
  nil)


;;;; -- Inline Execution --

(defun tests--inline-execution ()
  "Exercise running a queued job on the calling thread."
  (let ((release (tests--make-gate))
        (started (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test inline"
                          :maximum-concurrency 1)
      (let ((blocker (job-pool-submit
                      pool
                      (lambda (job)
                        (declare (ignore job))
                        (tests--gate-open started)
                        (tests--gate-await release))
                      :name "blocker"))
            (queued (job-pool-submit pool
                                     (lambda (job) (job-identifier job))
                                     :name "inline")))
        (test-assert (tests--gate-await started)
                     "the blocking job occupies the only worker")
        (test-assert (tests--wait-until
                      (lambda () (= (job-pool-queued-count pool) 1)))
                     "the inline candidate is queued")
        (test-assert (job-run-inline queued)
                     "an inline runner claims a queued job")
        (test-assert (eq (getf (job-snapshot queued) :state) :completed)
                     "an inline job publishes on the calling thread")
        (test-assert (string= (job-result queued) (job-identifier queued))
                     "an inline job publishes its own result")
        (test-assert (null (job-run-inline queued))
                     "an inline runner declines an already published job")
        (test-assert (zerop (job-pool-queued-count pool))
                     "an inline job leaves the queue")
        (tests--gate-open release)
        (tests--await-completed blocker))))
  nil)


;;;; -- Pool Shutdown --

(defun tests--close-and-reopen ()
  "Exercise closing a busy pool, detaching it, and reopening it."
  (let ((never (tests--make-gate))
        (started (tests--make-gate)))
    (let ((pool (make-job-pool :name "cl-jobpond test close"
                               :maximum-concurrency 2)))
      (let ((running (list (job-pool-submit
                            pool
                            (lambda (job)
                              (declare (ignore job))
                              (tests--gate-open started)
                              (tests--gate-await never))
                            :name "running")
                           (job-pool-submit
                            pool
                            (lambda (job)
                              (declare (ignore job))
                              (tests--gate-await never))
                            :name "running")))
            (queued (list (job-pool-submit pool
                                           (lambda (job) (job-identifier job))
                                           :name "queued")
                          (job-pool-submit pool
                                           (lambda (job) (job-identifier job))
                                           :name "queued"))))
        (test-assert (tests--gate-await started)
                     "a job is running when the pool closes")
        (test-assert (tests--wait-until
                      (lambda () (= (job-pool-active-count pool) 2)))
                     "both workers are busy when the pool closes")
        (test-assert (job-pool-close pool)
                     "closing a busy pool stops every thread")
        (test-assert (eq (job-pool-lifecycle-state pool) :closed)
                     "a stopped pool reports the closed state")
        (dolist (job (append running queued))
          (multiple-value-bind (snapshot terminal-p)
              (job-await job :timeout-seconds 60)
            (test-assert terminal-p
                         "closing makes every admitted job terminal")
            (test-assert (eq (getf snapshot :state) :aborted)
                         "closing aborts rather than finishes a job")
            (test-assert (eq (getf snapshot :cancellation-reason) :shutdown)
                         "closing records a shutdown reason")))
        (test-assert (zerop (job-pool-live-count pool))
                     "no job stays live after a close")
        (test-assert (zerop (job-pool-active-count pool))
                     "no job stays active after a close")
        (test-assert (job-pool-close pool)
                     "closing a closed pool succeeds again")
        (test-assert (signals job-pool-closed
                       (job-pool-submit pool (lambda (job) job)))
                     "a closed pool refuses new jobs")
        (test-assert (null (job-pool-detach pool))
                     "a closed pool drops its runtime state")
        (test-assert (null (job-pool-list-jobs pool))
                     "a detached pool retains no jobs")
        (test-assert (eq (job-pool-refresh pool) pool)
                     "refreshing a detached pool returns it")
        (test-assert (eq (job-pool-lifecycle-state pool) :open)
                     "a refreshed pool is open again")
        (test-assert (signals job-pool-detach-refused
                       (job-pool-detach pool))
                     "an open pool refuses to detach")
        (let ((job (job-pool-submit pool
                                    (lambda (job) (job-identifier job))
                                    :name "reopened")))
          (test-assert (eq (getf (tests--await-completed job) :state) :completed)
                       "a reopened pool runs jobs again")
          (test-assert (= (job-index job) 5)
                       "a reopened pool keeps counting admissions"))
        (test-assert (job-pool-close pool)
                     "the reopened pool closes again"))))
  nil)

(defun tests--detach-refusal ()
  "Exercise the refusal to detach a pool that never closed."
  (let ((pool (make-job-pool :name "cl-jobpond test detach"
                             :maximum-concurrency 1)))
    (let ((refusal (handler-case
                       (progn (job-pool-detach pool) nil)
                     (job-pool-detach-refused (condition) condition))))
      (test-assert refusal
                   "an open pool refuses to detach")
      (test-assert (eq (job-pool-detach-refused-reason refusal) :not-closed)
                   "the refusal explains that the pool is not closed"))
    (test-assert (job-pool-close pool)
                 "the pool closes after the refused detach")
    (test-assert (null (job-pool-detach pool))
                 "the closed pool detaches"))
  nil)

(defun tests--no-leaked-threads ()
  "Require that no test pool left a worker or monitor thread behind."
  (test-assert
   (tests--wait-until
    (lambda ()
      (notany (lambda (thread)
                (let ((name (thread-name thread)))
                  (and (stringp name)
                       (search "cl-jobpond test" name))))
              (all-threads))))
   "every test pool thread has exited")
  nil)


;;;; -- Entry Point --

(defun tests--ancestry-cascade ()
  "Exercise job ancestry, descendant lookup, and cascading cancellation."
  (let ((started (tests--make-gate))
        (never (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test cascade"
                          :maximum-concurrency 4)
      (let* ((root (job-pool-submit pool
                                    (lambda (job)
                                      (declare (ignore job))
                                      (tests--gate-open started)
                                      (tests--gate-await never
                                                         :timeout-seconds 60)
                                      :root-finished)
                                    :name "root"
                                    :root-identifier "tree"))
             (root-identifier (job-identifier root))
             (child (job-pool-submit pool
                                     (lambda (job)
                                       (declare (ignore job))
                                       (tests--gate-await never
                                                          :timeout-seconds 60)
                                       :child-finished)
                                     :name "child"
                                     :owner-identifiers (list root-identifier)
                                     :root-identifier "tree"))
             (grandchild (job-pool-submit
                          pool
                          (lambda (job)
                            (declare (ignore job))
                            (tests--gate-await never :timeout-seconds 60)
                            :grandchild-finished)
                          :name "grandchild"
                          :owner-identifiers
                          (list root-identifier (job-identifier child))
                          :root-identifier "tree"))
             (stranger (job-pool-submit pool
                                        (lambda (job)
                                          (declare (ignore job))
                                          :stranger-finished)
                                        :name "stranger")))
        (test-assert (equal (job-owner-identifiers grandchild)
                            (list root-identifier (job-identifier child)))
                     "a job records its ancestors outermost first")
        (test-assert (string= (job-root-identifier child) "tree")
                     "a job records the tree it belongs to")
        (test-assert (null (job-owner-identifiers stranger))
                     "a job with no ancestors records none")
        (test-assert (tests--gate-await started)
                     "the root job started")
        (test-assert (eq (job-state stranger) :completed)
                     "an unrelated job is unaffected by the subtree")
        (let ((descendants (job-pool-descendant-jobs pool root-identifier)))
          (test-assert (= (length descendants) 2)
                       "descendant lookup finds the whole subtree")
          (test-assert (and (member child descendants :test #'eq)
                            (member grandchild descendants :test #'eq))
                       "descendant lookup finds children and grandchildren")
          (test-assert (not (member stranger descendants :test #'eq))
                       "descendant lookup excludes unrelated jobs")
          (test-assert (not (member root descendants :test #'eq))
                       "descendant lookup excludes the job itself"))
        (multiple-value-bind (accepted-p cascaded)
            (job-cancel root :reason :superseded :cascade-p t)
          (test-assert accepted-p
                       "a cascading cancellation accepts the job itself")
          (test-assert (= cascaded 2)
                       "a cascading cancellation reports the descendants it took"))
        (test-assert (tests--wait-until
                      (lambda ()
                        (and (job-terminal-p root)
                             (job-terminal-p child)
                             (job-terminal-p grandchild)))
                      :timeout-seconds 60)
                     "every job in the subtree reaches a terminal state")
        (test-assert (eq (job-state root) :aborted)
                     "the cancelled root is aborted")
        (test-assert (eq (job-state child) :aborted)
                     "a cascaded child is aborted")
        (test-assert (eq (job-state grandchild) :aborted)
                     "a cascaded grandchild is aborted")
        (test-assert (eq (job-cancellation-reason grandchild) :superseded)
                     "a cascaded job records the reason the root was given")
        (test-assert (eq (job-state stranger) :completed)
                     "an unrelated job survives the cascade")
        (test-assert (null (job-pool-descendant-jobs pool root-identifier))
                     "a cancelled subtree has no live descendants left")
        (tests--gate-open never))))
  (with-test-pool (pool :name "cl-jobpond test ancestry validation")
    (dolist (entry (list (list :function #'identity :owner-identifiers "root")
                         (list :function #'identity :owner-identifiers '(""))
                         (list :function #'identity :owner-identifiers '(:root))
                         (list :function #'identity :root-identifier "")
                         (list :function #'identity :root-identifier :tree)))
      (test-assert
       (handler-case
           (progn (job-pool-submit-batch pool (list entry)) nil)
         (job-pool-invalid-entry ()
           t))
       (format nil "~S is refused as a malformed ancestry entry" entry))
      (test-assert (zerop (job-pool-live-count pool))
                   "a refused ancestry entry admits nothing")))
  nil)

(defun run-tests ()
  "Run every cl-jobpond regression test."
  (setf *test-count* 0)
  (tests--execution)
  (tests--identifiers)
  (tests--concurrency-bound)
  (tests--admission-bounds)
  (tests--invalid-limits)
  (tests--interrupt-guard)
  (tests--ancestry-cascade)
  (tests--running-cancellation)
  (tests--queued-cancellation)
  (tests--cooperative-cancellation)
  (tests--late-completion-downgrade)
  (tests--pool-deadline)
  (tests--job-deadline)
  (tests--await-timeout)
  (tests--progress)
  (tests--listeners)
  (tests--retention-ring)
  (tests--inline-execution)
  (tests--close-and-reopen)
  (tests--detach-refusal)
  (tests--no-leaked-threads)
  (format t "~&~:D cl-jobpond tests passed.~%" *test-count*)
  nil)
