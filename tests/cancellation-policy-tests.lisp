(in-package #:cl-jobpond/tests)

;;;; -- Cooperative Cancellation Policy --

(defclass tests--cooperative-job (cl-jobpond:job)
  ()
  (:documentation "A job whose body owns cancellation polling and cleanup."))

(defmethod cl-jobpond:job-interrupt-on-cancellation-p
    ((job tests--cooperative-job))
  "Leave cancellation delivery to this job's body."
  nil)

(defun tests--cooperative-body (started cleaned &key returned check-p release)
  "Return a polling body that records its cleanup and optional normal return."
  (lambda (job)
    (unwind-protect
         (progn
           (tests--gate-open started)
           (loop until (job-cancellation-requested-p job) do (sleep 0.002))
           (when release (tests--gate-await release))
           (when check-p (job-check-cancellation job))
           (when returned (tests--gate-open returned))
           :late-value)
      (tests--gate-open cleaned))))

(defun tests--cancellation-policy-return ()
  "Exercise cooperative polling, explicit cancellation checks and worker reuse."
  (with-test-pool (pool :name "cl-jobpond test cancellation policy"
                        :maximum-concurrency 1
                        :job-class 'tests--cooperative-job)
    (dolist (check-p '(nil t))
      (let* ((started (tests--make-gate))
             (cleaned (tests--make-gate))
             (returned (tests--make-gate))
             (job (job-pool-submit
                   pool (tests--cooperative-body started cleaned
                                                 :returned returned :check-p check-p))))
        (test-assert (tests--gate-await started) "the cooperative body starts")
        (test-assert (job-cancel job :reason :superseded)
                     "the cooperative job accepts its first cancellation")
        (test-assert (not (job-cancel job :reason :shutdown))
                     "repeated cooperative cancellation is declined")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "cooperative cancellation publishes aborted")
          (test-assert (eq (getf snapshot :cancellation-reason) :superseded)
                       "cooperative cancellation keeps the first reason")
          (test-assert (null (getf snapshot :result))
                       "a cancelled normal return cannot publish its value"))
        (test-assert (tests--gate-await cleaned) "cooperative cleanup finishes")
        (test-assert (eql (tests--gate-await returned :timeout-seconds 0) (not check-p))
                     "only polling without an explicit check returns normally")))
    (let ((later (job-pool-submit pool (lambda (job) (declare (ignore job)) :reused))))
      (test-assert (eq (getf (tests--await-completed later) :state) :completed)
                   "a worker accepts later work after cooperative cancellation")))
  nil)

(defun tests--cancellation-policy-deadline ()
  "Exercise monitor cancellation and cleanup without an asynchronous interrupt."
  (let ((started (tests--make-gate))
        (cleaned (tests--make-gate))
        (returned (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test cooperative deadline"
                          :maximum-concurrency 1
                          :maximum-runtime-milliseconds 50
                          :job-class 'tests--cooperative-job)
      (let ((job (job-pool-submit
                  pool (tests--cooperative-body started cleaned :returned returned))))
        (test-assert (tests--gate-await started) "the deadline body starts")
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "a cooperative deadline publishes aborted")
          (test-assert (eq (getf snapshot :cancellation-reason) :timeout)
                       "the monitor records its timeout reason"))
        (test-assert (tests--gate-await returned) "the deadline body observes cancellation")
        (test-assert (tests--gate-await cleaned) "the deadline body cleans up"))))
  nil)

(defun tests--cancellation-policy-cascade-and-queue ()
  "Exercise cooperative subtree cancellation with a queued descendant."
  (with-test-pool (pool :name "cl-jobpond test cooperative cascade"
                        :maximum-concurrency 2
                        :job-class 'tests--cooperative-job)
    (let* ((parent-started (tests--make-gate))
           (parent-cleaned (tests--make-gate))
           (child-started (tests--make-gate))
           (child-cleaned (tests--make-gate))
           (release (tests--make-gate))
           (queued-ran-p nil)
           (parent (job-pool-submit
                    pool (tests--cooperative-body parent-started parent-cleaned
                                                 :release release)))
           (child (job-pool-submit
                   pool (tests--cooperative-body child-started child-cleaned
                                                :release release)
                   :owner-identifiers (list (job-identifier parent))))
           (queued (job-pool-submit
                    pool (lambda (job) (declare (ignore job)) (setf queued-ran-p t))
                    :owner-identifiers (list (job-identifier parent)))))
      (test-assert (tests--gate-await parent-started) "the subtree parent starts")
      (test-assert (tests--gate-await child-started) "the subtree child starts")
      (multiple-value-bind (accepted-p descendants)
          (job-cancel parent :reason :discarded :cascade-p t)
        (test-assert accepted-p "cooperative subtree cancellation is accepted")
        (test-assert (and (= (length descendants) 2)
                          (member child descendants) (member queued descendants))
                     "cascade accepts running and queued cooperative descendants"))
      (tests--gate-open release)
      (dolist (job (list parent child queued))
        (let ((snapshot (tests--await-completed job)))
          (test-assert (eq (getf snapshot :state) :aborted)
                       "every cancelled subtree job becomes aborted")
          (test-assert (eq (getf snapshot :cancellation-reason) :discarded)
                       "every subtree job receives the cascade reason")))
      (test-assert (not queued-ran-p) "a cancelled queued cooperative body never runs")
      (test-assert (tests--gate-await parent-cleaned) "the cancelled parent cleans up")
      (test-assert (tests--gate-await child-cleaned) "the cancelled child cleans up")))
  nil)

(defun tests--cancellation-policy-shutdown ()
  "Exercise cooperative pool shutdown with prompt body cleanup."
  (let ((started (tests--make-gate))
        (cleaned (tests--make-gate))
        (returned (tests--make-gate)))
    (with-test-pool (pool :name "cl-jobpond test cooperative shutdown"
                          :maximum-concurrency 1
                          :job-class 'tests--cooperative-job)
      (let ((job (job-pool-submit
                  pool (tests--cooperative-body started cleaned :returned returned))))
        (test-assert (tests--gate-await started) "the shutdown body starts")
        (test-assert (job-pool-close pool) "cooperative shutdown closes the pool")
        (test-assert (eq (job-state job) :aborted) "shutdown publishes an aborted job")
        (test-assert (eq (job-cancellation-reason job) :shutdown)
                     "shutdown records its reason")
        (test-assert (tests--gate-await returned) "the shutdown body observes cancellation")
        (test-assert (tests--gate-await cleaned) "the shutdown body finishes cleanup"))))
  nil)

(defun tests--cancellation-policy-repeated ()
  "Repeat cooperative cancellation and cleanup on one reusable worker."
  (with-test-pool (pool :name "cl-jobpond test repeated cooperative cancellation"
                        :maximum-concurrency 1
                        :job-class 'tests--cooperative-job)
    (dotimes (index 200)
      (let* ((started (tests--make-gate))
             (cleaned (tests--make-gate))
             (returned (tests--make-gate))
             (job (job-pool-submit
                   pool (tests--cooperative-body started cleaned :returned returned))))
        (test-assert (tests--gate-await started) "each repeated body starts")
        (test-assert (job-cancel job) "each repeated cancellation is accepted")
        (test-assert (not (job-cancel job)) "each duplicate cancellation is declined")
        (test-assert (eq (getf (tests--await-completed job) :state) :aborted)
                     "each repeated cancellation publishes aborted")
        (test-assert (tests--gate-await returned)
                     "each repeated body returns after observing cancellation")
        (test-assert (tests--gate-await cleaned) "each repeated cleanup finishes"))))
  nil)

(defun tests--cancellation-policy ()
  "Run the subclass cancellation-policy contracts."
  (tests--cancellation-policy-return)
  (tests--cancellation-policy-deadline)
  (tests--cancellation-policy-cascade-and-queue)
  (tests--cancellation-policy-shutdown)
  (tests--cancellation-policy-repeated)
  nil)
