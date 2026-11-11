(in-package #:cl-jobpond)

;;;; -- Cancellation Policy --

(defgeneric job-interrupt-on-cancellation-p (job)
  (:documentation
   "Return true when cancellation should asynchronously interrupt JOB's worker.

The default method returns T. A subclass may return NIL for cooperative
cancellation. Its body must poll JOB-CANCELLATION-REQUESTED-P or call
JOB-CHECK-CANCELLATION and unwind promptly, including during pool shutdown.
Methods run on the delivery thread, outside ownership and lifecycle locks,
and must return promptly without signalling. Policy is stable for the run.
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
  "Return live linked descendants under the ownership lock.
Traverse terminal intermediates: their branches may still own live jobs."
  (mapcan (lambda (child)
            (append (unless (job-terminal-p child) (list child))
                    (job--linked-descendants child)))
          (job--child-jobs job)))

(defun job--prune-settled-branch (job)
  "Release strong links only when JOB and its entire branch have settled."
  (bordeaux-threads:with-recursive-lock-held (*job-tree-lock*)
    (loop for current = job then parent
          for parent = (job-parent-job current)
          while (and parent (job-terminal-p current)
                     (null (job--child-jobs current)))
          do (setf (job--child-jobs parent)
                   (remove current (job--child-jobs parent) :test #'eq)
                   (job-parent-job current) nil))))

(defun job-cancel (job &key (reason :cancelled) cascade-p)
  "Request cancellation without waiting for jobs or terminal hooks.
Return whether JOB accepted cancellation and the selected live descendants,
including jobs already cancelling or publishing. Callers own any bounded waits.
Admission and subtree selection share an ownership lock. Mark every target
before sending interrupts, and send every interrupt before queued hooks run.
Queued publication runs on a separate thread; pool records and branch links
retain its jobs until publication finishes. A terminal parent can therefore
still own unfinished descendants."
  (let ((targets nil) (accepted nil) (notifications nil) (root-accepted-p nil)
        (start-lock (make-lock "cl-jobpond cancellation admission")))
    ;; The delivery thread cannot run until selection, registration and marking
    ;; have committed. Create it before changing cancellation state, so failure
    ;; to create a thread leaves the request retryable without partial marks.
    (with-lock-held (start-lock)
      (bordeaux-threads:with-recursive-lock-held (*job-tree-lock*)
        (setf targets
              (remove-duplicates
               (cons job
                     (when cascade-p
                       (append (job-pool-descendant-jobs
                                (job-pool job) (job-identifier job))
                               (job--linked-descendants job))))
               :test #'eq))
        (let ((candidates (remove-if-not #'job--cancellable-p targets)))
          (when candidates
            (job--start-cancellation-delivery
             candidates start-lock
             (lambda ()
                (dolist (notification (nreverse notifications))
                  ;; A worker can exit between its liveness check and delivery.
                  (handler-case (funcall notification)
                    (error () nil)))
               (setf notifications nil)
               (loop while accepted
                     do (job--publish-cancelled-queued (pop accepted) reason))))
            (dolist (target candidates)
              (multiple-value-bind (cancel-p notification)
                  (job--mark-cancellation target reason)
                (when cancel-p (push target accepted))
                (when notification (push notification notifications)))))))
      (setf root-accepted-p (if (member job accepted :test #'eq) t nil)))
    (values root-accepted-p (remove job targets :test #'eq))))

(defun job--cancellable-p (job)
  "Return whether JOB can accept a first cancellation under the ownership lock."
  (with-lock-held ((job--lock job))
    (not (or (job--terminal-state-p (job-state job))
             (job--publication-claimed-p job)
             (job-cancellation-reason job)))))

(defun job--start-cancellation-delivery (targets start-lock function)
  "Register one delivery owner for TARGETS before releasing the ownership lock.
The caller holds START-LOCK until marking commits. Closing any affected pool
then includes this owner in its bounded shutdown budget."
  (let* ((pools (remove-duplicates (mapcar #'job-pool targets) :test #'eq))
         (thread
           (make-thread
            (lambda ()
              (with-lock-held (start-lock))
              (unwind-protect
                   (funcall function)
                (dolist (pool pools)
                  (with-lock-held ((job-pool--lock pool))
                    (setf (job-pool--cancellation-threads pool)
                          (remove (current-thread) (job-pool--cancellation-threads pool)
                                  :test #'eq))
                    (jobpond--condition-broadcast (job-pool--condition-variable pool))))))
            :name "cl-jobpond cancellation delivery")))
    (dolist (pool pools)
      (with-lock-held ((job-pool--lock pool))
        (push thread (job-pool--cancellation-threads pool))))
    thread))

(defun job--publish-cancelled-queued (job reason)
  "Publish JOB after its cancellation subtree has been marked."
  (when (with-lock-held ((job--lock job))
          (and (eq (job-state job) :queued)
               (job-cancellation-reason job)))
    (job--publish-terminal
     job :aborted nil
     :report      (format nil "Job ~A was ~(~A~) before it started."
                          (job-identifier job) reason))))

(defun job--mark-cancellation (job reason)
  "Mark JOB under the ownership lock; return acceptance and an interrupt thunk.
The thunk must run outside ownership and lifecycle locks."
  (let ((thread nil) (run-token nil) (cancel-p nil))
    (with-lock-held ((job--lock job))
      (unless (or (job--terminal-state-p (job-state job))
                  (job--publication-claimed-p job)
                  (job-cancellation-reason job))
        (setf (job-cancellation-reason job) reason
              thread (job--thread job)
              run-token (job-run-token job)
              cancel-p t)))
    (when cancel-p
      (let ((pool (job-pool job)))
        (with-lock-held ((job-pool--lock pool))
          (setf (job-pool--queue pool)
                (remove job (job-pool--queue pool) :test #'eq))
          (jobpond--condition-broadcast (job-pool--condition-variable pool)))))
    (values cancel-p
            (when (and cancel-p thread run-token)
              (lambda ()
                (when (and (thread-alive-p thread)
                           (job-interrupt-on-cancellation-p job))
                  (interrupt-thread
                   thread
                   (lambda ()
                     (when (job--cancellation-interrupt-applicable-p job run-token)
                       (error 'job-aborted
                              :message    (format nil "Job ~A was ~(~A~)."
                                                  (job-identifier job) reason)
                              :identifier (job-identifier job)
                              :reason     reason))))))))))
