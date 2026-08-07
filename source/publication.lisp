(in-package #:cl-jobpond)

;;;; -- Terminal Retention --

(defun job-pool--retain-terminal-locked (pool job)
  "Account for terminal JOB in POOL while JOB's lifecycle lock is held.

The live count drops by one and JOB's identifier joins the retention ring. When
the ring exceeds the pool retention limit, the oldest terminal job leaves the
lookup table, so a long-running pool keeps a bounded number of finished jobs.

Locks are always taken job first and pool second, and never the other way, so
this nesting cannot deadlock against admission or the worker loop."
  (unless (job--retained-p job)
    (setf (job--retained-p job) t)
    (with-lock-held ((job-pool--lock pool))
      (when (plusp (job-pool--live-count pool))
        (decf (job-pool--live-count pool)))
      (setf (job-pool--terminal-identifiers pool)
            (nconc (job-pool--terminal-identifiers pool)
                   (list (job-identifier job))))
      (loop while (> (length (job-pool--terminal-identifiers pool))
                     (job-pool-terminal-retention-limit pool))
            do (remhash (pop (job-pool--terminal-identifiers pool))
                        (job-pool--jobs pool)))
      (jobpond--condition-broadcast (job-pool--condition-variable pool))))
  nil)


;;;; -- Lifecycle Events --

(defun job--lifecycle-event (job status)
  "Return JOB's lifecycle event plist for STATUS.

The job itself is carried on the event. A listener that wants more than the
status has to reach the job for it, and looking one up by identifier would fail
exactly when terminal retention has already evicted it."
  (list :job job
        :identifier (job-identifier job)
        :index (job-index job)
        :name (job-name job)
        :status status))


;;;; -- Host Terminal Records --

(defun job--resolve-terminal-record (job state result report)
  "Return JOB's terminal record for STATE as (values result report state).

The library owns the publication claim and the state machine; a host that needs
to own what a terminal job *carries* supplies :TERMINAL-RESULT-FUNCTION. The hook
is called with the job, the resolved state, the result being published, and the
report, and returns its own result, report, and state.

It runs inside the claim and outside JOB's lifecycle lock, so exactly one writer
ever runs it and it may take locks or do input and output. That is what makes it
the right place for work whose side effects must not be duplicated, such as
writing a result artifact named after the job.

A returned state replaces the resolved one only when it is itself terminal, so a
hook cannot revive a job by answering :RUNNING."
  (let ((hook (job-terminal-result-function job)))
    (if hook
        (multiple-value-bind (hook-result hook-report hook-state)
            (funcall hook job state result report)
          (values hook-result
                  hook-report
                  (if (job--terminal-state-p hook-state) hook-state state)))
        (values result report state))))

(defun job--force-terminal-record (job state report)
  "Return the host terminal record for a forced failure, or NIL when it fails.

The forced path must not be able to fail, so the hook runs inside a handler that
discards every condition, and its state and report answers are ignored: this path
has already decided both. A host whose hook fails here loses the detail of the
record, never the terminal state."
  (let ((hook (job-terminal-result-function job)))
    (if hook
        (handler-case
            (values (funcall hook job state nil report))
          (serious-condition ()
            nil))
        nil)))


;;;; -- Terminal Publication --

(defun job--publish-terminal (job requested-state result &key report)
  "Claim and publish exactly one terminal RESULT for JOB, returning T on success.

Publication is single-writer. A job body returning normally, a deadline
cancellation, and pool shutdown can all reach this function for the same job, so
the terminal-state test and the publication claim are taken together under JOB's
lifecycle lock. Only the writer that takes the claim installs a result; every
later writer returns NIL and leaves the published result alone.

*TERMINAL-PUBLICATION-JOB* is bound to JOB across the whole publication. A
cancellation interrupt aimed at JOB that arrives during this window sees its own
job in that variable and declines to unwind the writer, so a cancellation can
never leave a half-published terminal state behind.

A cancellation reason recorded before the claim downgrades REQUESTED-STATE to
:ABORTED and discards RESULT, so a job cancelled just as it finished never
reports success.

The job's :TERMINAL-RESULT-FUNCTION, when it has one, then decides what the
terminal record actually contains. See JOB--RESOLVE-TERMINAL-RECORD."
  (let ((*terminal-publication-job* job)
        (publish-p nil)
        (state requested-state)
        (reason nil)
        (final-result result)
        (final-report report)
        (event nil))
    (with-lock-held ((job--lock job))
      (unless (or (job--terminal-state-p (job-state job))
                  (job--publication-claimed-p job))
        (setf reason (job-cancellation-reason job))
        (when reason
          (setf state :aborted))
        (setf (job--publication-claimed-p job) t
              publish-p t)))
    (when publish-p
      (handler-case
          (progn
            (when (and (eq state :aborted) (not (eq requested-state :aborted)))
              (setf final-result nil
                    final-report
                    (or final-report
                        (format nil "Job ~A was ~(~A~) as it finished."
                                (job-identifier job) reason))))
            (multiple-value-setq (final-result final-report state)
              (job--resolve-terminal-record job state final-result final-report))
            (setf final-report
                  (if final-report
                      (jobpond--bounded-string final-report)
                      nil))
            (with-lock-held ((job--lock job))
              (job--compact-progress job state)
              (setf (job-state job) state
                    (job--publication-claimed-p job) nil
                    (job-result job) final-result
                    (job-condition-report job) final-report
                    (job-ended-at job) (get-internal-real-time)
                    (job--thread job) nil
                    (job-run-token job) nil
                    (job-deadline job) nil
                    event (job--lifecycle-event job state))
              (job-pool--retain-terminal-locked (job-pool job) job)
              (jobpond--condition-broadcast (job--condition-variable job)))
            (job-pool-emit (job-pool job) :job-lifecycle event))
        (serious-condition (condition)
          (job--force-terminal-failure job condition condition))))
    publish-p))

(defun job--force-terminal-failure
    (job execution-condition publication-condition)
  "Force JOB terminal when normal terminal publication itself failed.

This is the last line of defence. It writes the smallest possible terminal state
without calling anything that can fail, so a job never stays live after its
worker gave up on it. Waiters are always woken, even when the state was already
terminal, so a lost broadcast cannot strand them."
  (let* ((reason (with-lock-held ((job--lock job))
                   (job-cancellation-reason job)))
         (state (if reason :aborted :failed))
         (report
           (jobpond--bounded-string
            (format nil "Job failure: ~A; publication failure: ~A"
                    execution-condition publication-condition)))
         (result (job--force-terminal-record job state report))
         (event nil))
    (with-lock-held ((job--lock job))
      (unless (job--terminal-state-p (job-state job))
        (job--compact-progress job state)
        (setf (job-state job) state
              (job--publication-claimed-p job) nil
              (job-result job) result
              (job-condition-report job) report
              (job-ended-at job) (get-internal-real-time)
              (job--thread job) nil
              (job-run-token job) nil
              (job-deadline job) nil
              event (job--lifecycle-event job state))
        (job-pool--retain-terminal-locked (job-pool job) job)))
    (with-lock-held ((job--lock job))
      (jobpond--condition-broadcast (job--condition-variable job)))
    (when event
      (job-pool-emit (job-pool job) :job-lifecycle event))
    nil))


;;;; -- Job Execution --

(defun job--start-locked (job token)
  "Move queued JOB to :RUNNING under TOKEN and return T when it started.

Returns NIL when the job was cancelled or already started, in which case the
caller must not run its body. The caller holds JOB's lifecycle lock."
  (if (and (eq (job-state job) :queued)
           (null (job-cancellation-reason job)))
      (let ((now (get-internal-real-time))
            (runtime (job-maximum-runtime-milliseconds job)))
        (setf (job-state job) :running
              (job--thread job) (current-thread)
              (job-run-token job) token
              (job-started-at job) now
              (job-deadline job)
              (if (plusp runtime)
                  (+ now
                     (round (* runtime internal-time-units-per-second) 1000))
                  nil))
        t)
      nil))

(defun job--execute (job)
  "Run JOB on the calling thread and publish exactly one terminal result.

The job body is called with JOB as its only argument. Returning normally
publishes :COMPLETED with the returned value, signalling JOB-ABORTED publishes
:ABORTED, and signalling any other error publishes :FAILED with a bounded report.

*CURRENT-JOB* and *CURRENT-RUN-TOKEN* are bound around the body so a cancellation
interrupt delivered to this thread can prove it still belongs to this run."
  (let ((pool (job-pool job))
        (token (job--next-run-token))
        (started-p nil))
    (with-lock-held ((job--lock job))
      (setf started-p (job--start-locked job token))
      (when started-p
        (job--set-progress-state job :running)))
    (when started-p
      (job-pool-emit pool :job-lifecycle (job--lifecycle-event job :started))
      (let ((*current-job* job)
            (*current-run-token* token))
        (handler-case
            (job--publish-terminal
             job
             :completed
             (funcall (job--body-function job) job))
          (job-aborted (condition)
            (job--publish-terminal job
                                   :aborted
                                   nil
                                   :report (princ-to-string condition)))
          (error (condition)
            (job--publish-terminal job
                                   :failed
                                   nil
                                   :report (princ-to-string condition)))))))
  nil)
