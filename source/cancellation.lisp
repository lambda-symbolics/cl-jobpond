(in-package #:cl-jobpond)

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

(defun job-cancel (job &key (reason :cancelled) cascade-p)
  "Request cancellation of JOB for REASON and return T when this call accepted it.

With CASCADE-P, every live descendant of JOB is cancelled too, and the number of
descendants this call accepted is returned as a second value. JOB is cancelled
first, so a body that spawns children stops adding to the subtree before the
subtree is walked. A descendant admitted after the walk is not reached, so a
caller that must guarantee an empty subtree cancels and then re-checks.

Cancellation is first-writer: the first accepted request records REASON, and
later requests return NIL rather than recording a second reason or interrupting
a second time. A job that is already terminal, or that has claimed its terminal
publication, is never cancelled.

A queued job leaves the pool queue and publishes :ABORTED immediately, because no
worker will ever pick it up. A running job is interrupted on its worker thread
with a closure guarded by JOB--CANCELLATION-INTERRUPT-APPLICABLE-P, so a delayed
interrupt cannot strike whichever job that worker runs next.

An interrupt is best effort. A body that blocks where the host cannot deliver
interrupts stops at its next JOB-CHECK-CANCELLATION or JOB-REPORT-PROGRESS call
instead."
  (let ((accepted-p (job--cancel-one job reason))
        (cascaded 0))
    (when cascade-p
      (dolist (descendant (job-pool-descendant-jobs (job-pool job)
                                                    (job-identifier job)))
        (when (job--cancel-one descendant reason)
          (incf cascaded))))
    (values accepted-p cascaded)))

(defun job--cancel-one (job reason)
  "Request cancellation of exactly JOB for REASON, ignoring any descendants."
  (let ((pool (job-pool job))
        (thread nil)
        (run-token nil)
        (queued-p nil)
        (cancel-p nil))
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
      (when queued-p
        (job--publish-terminal
         job
         :aborted
         nil
         :report (format nil "Job ~A was ~(~A~) before it started."
                         (job-identifier job) reason)))
      (when (and thread run-token (thread-alive-p thread))
        (interrupt-thread
         thread
         (lambda ()
           (when (job--cancellation-interrupt-applicable-p job run-token)
             (error 'job-aborted
                    :message (format nil "Job ~A was ~(~A~)."
                                     (job-identifier job) reason)
                    :identifier (job-identifier job)
                    :reason reason))))))
    cancel-p))
