(in-package #:cl-jobpond)

;;;; -- Cancellation Policy --

(defgeneric job-interrupt-on-cancellation-p (job)
  (:documentation
   "Return true when cancellation should asynchronously interrupt JOB's worker.

The default method returns T. A subclass may return NIL for cooperative
cancellation. Its body must poll JOB-CANCELLATION-REQUESTED-P or call
JOB-CHECK-CANCELLATION and unwind promptly, including during pool shutdown.
Methods run on the cancelling thread, outside lifecycle locks, and must return
promptly without signalling. Policy must be stable throughout the job's run.
Queued cancellation and terminal publication do not consult this policy."))

(defmethod job-interrupt-on-cancellation-p ((job job))
  "Use asynchronous cancellation for ordinary jobs."
  t)


;;;; -- Cancellation Interrupt Guard --

(defun job--cancellation-interrupt-applicable-p (job run-token)
  "Return T when a delivered cancellation interrupt still belongs to JOB's run.

An interrupt is prepared on the controller's thread and delivered on the worker
thread at some later, unspecified moment. By then the worker may have finished
JOB and picked up a completely different job, so the interrupt must prove three
things about the state it lands in before it is allowed to unwind anything.

The worker must still be running JOB, which *CURRENT-JOB* reports. JOB must not
be publishing its terminal result, which *TERMINAL-PUBLICATION-JOB* reports, so
an interrupt cannot tear a half-written terminal state. Finally the run token
bound on the worker must be the exact token captured when cancellation was
requested. Tokens are unique across the image, so a token match proves the
interrupt targets this run of this job and not a later one, even if the same job
object were somehow presented to a worker twice.

Callers run this on the interrupted thread, where the dynamic bindings belong to
the run being examined."
  (if (and (eq *current-job* job)
           (not (eq *terminal-publication-job* job))
           (stringp *current-run-token*)
           (stringp run-token)
           (string= *current-run-token* run-token))
      t
      nil))


;;;; -- Cancellation --

(defun job-pool-descendant-jobs (pool identifier)
  "Return POOL's live jobs descended from the job named IDENTIFIER.

A job records every ancestor identifier, so descent is a membership test and one
pass finds a whole subtree however deep it is. Terminal jobs are left out: there
is nothing left to cancel in them."
  (remove-if-not
   (lambda (job)
     (and (not (job--terminal-state-p (job-state job)))
          (member identifier (job-owner-identifiers job) :test #'string=)))
   (job-pool-list-jobs pool)))

(defun job--linked-descendants (job)
  "Return live jobs linked below JOB, including descendants in other pools."
  (let ((children (with-lock-held ((job--lock job))
                    (copy-list (job--child-jobs job)))))
    (mapcan (lambda (child)
              (cons child (job--linked-descendants child)))
            (remove-if #'job-terminal-p children))))

(defun job-cancel (job &key (reason :cancelled) cascade-p)
  "Cancel JOB and mark its complete subtree before queued hooks run."
  (let* ((same-pool (and cascade-p
                         (job-pool-descendant-jobs (job-pool job)
                                                   (job-identifier job))))
         (linked    (and cascade-p (job--linked-descendants job)))
         (targets   (remove-duplicates (cons job (append same-pool linked))
                                       :test #'eq))
         (accepted  nil))
    (dolist (target targets)
      (when (job--cancel-one target reason :publish-queued-p nil)
        (push target accepted)))
    (dolist (target accepted)
      (job--publish-cancelled-queued target reason))
    (values (member job accepted :test #'eq)
            (nreverse (remove job accepted :test #'eq)))))

(defun job--publish-cancelled-queued (job reason)
  "Publish JOB after its cancellation subtree has been marked."
  (when (with-lock-held ((job--lock job))
          (and (eq (job-state job) :queued)
               (job-cancellation-reason job)))
    (job--publish-terminal
     job :aborted nil
     :report      (format nil "Job ~A was ~(~A~) before it started."
                          (job-identifier job) reason))))

(defun job--cancel-one (job reason &key (publish-queued-p t))
  "Request cancellation of exactly JOB for REASON, ignoring descendants."
  (let ((pool      (job-pool job))
        (thread    nil)
        (run-token nil)
        (queued-p  nil)
        (cancel-p  nil))
    (with-lock-held ((job--lock job))
      (unless (or (job--terminal-state-p (job-state job))
                  (job--publication-claimed-p job)
                  (job-cancellation-reason job))
        (setf (job-cancellation-reason job) reason
              thread (job--thread job)
              run-token (job-run-token job)
              queued-p (eq (job-state job) :queued)
              cancel-p t)))
    (when cancel-p
      (with-lock-held ((job-pool--lock pool))
        (setf (job-pool--queue pool)
              (remove job (job-pool--queue pool) :test #'eq))
        (jobpond--condition-broadcast (job-pool--condition-variable pool)))
      (when (and publish-queued-p queued-p)
        (job--publish-cancelled-queued job reason))
      (when (and thread run-token (thread-alive-p thread)
                 (job-interrupt-on-cancellation-p job))
        (interrupt-thread
         thread
         (lambda ()
           (when (job--cancellation-interrupt-applicable-p job run-token)
             (error 'job-aborted
                    :message    (format nil "Job ~A was ~(~A~)."
                                        (job-identifier job) reason)
                    :identifier (job-identifier job)
                    :reason     reason))))))
    cancel-p))
