(in-package #:cl-jobpond)

;;;; -- Reusable Workers --

(defun job-pool--worker-loop (pool)
  "Run queued jobs on one reusable worker until POOL shuts down.

A worker claims a job only while the pool has fewer running jobs than its
concurrency bound, so the bound holds even when more workers exist than the
bound allows. The worker sleeps on the pool condition variable between jobs, so
admission and shutdown both wake it without polling.

The nested handlers guarantee that a job never stays live after its worker gave
up on it: normal execution publishes a result, an escaping condition publishes a
failure, and a condition escaping that publication forces a terminal failure."
  (loop
    (let ((job nil))
      (with-lock-held ((job-pool--lock pool))
        (loop
          (when (job-pool--shutdown-p pool)
            (return-from job-pool--worker-loop nil))
          (when (and (job-pool--queue pool)
                     (< (job-pool--active-count pool)
                        (job-pool-maximum-concurrency pool)))
            (setf job (pop (job-pool--queue pool)))
            (incf (job-pool--active-count pool))
            (return))
          (condition-wait (job-pool--condition-variable pool)
                          (job-pool--lock pool))))
      (unwind-protect
           (handler-case
               (job--execute job)
             (serious-condition (condition)
               (handler-case
                   (unless (job-terminal-p job)
                     (unless (job--publish-terminal
                              job
                              :failed
                              nil
                              :report (princ-to-string condition))
                       (unless (job-terminal-p job)
                         (job--force-terminal-failure job condition condition))))
                 (serious-condition (publication-condition)
                   (job--force-terminal-failure
                    job condition publication-condition)))))
        (with-lock-held ((job-pool--lock pool))
          (decf (job-pool--active-count pool))
          (jobpond--condition-broadcast
           (job-pool--condition-variable pool)))))))

(defun job-pool--ensure-workers-locked (pool)
  "Start enough reusable workers for POOL while its lock is held.

Dead workers are forgotten first. Lowering the concurrency bound leaves excess
workers alive and sleeping, because the worker claim path enforces the new bound
without interrupting a job or retiring a reusable thread in place."
  (setf (job-pool--worker-threads pool)
        (remove-if-not #'thread-alive-p (job-pool--worker-threads pool)))
  (when (eq (job-pool-lifecycle-state pool) :open)
    (loop repeat (max 0
                      (- (job-pool-maximum-concurrency pool)
                         (length (job-pool--worker-threads pool))))
          for index from (length (job-pool--worker-threads pool))
          do (push (make-thread
                    (lambda () (job-pool--worker-loop pool))
                    :name (format nil "~A worker ~D"
                                  (job-pool-name pool) (1+ index)))
                   (job-pool--worker-threads pool))))
  nil)

(defun job-pool--ensure-workers (pool)
  "Start reusable workers until POOL has one per unit of its concurrency bound."
  (with-lock-held ((job-pool--lock pool))
    (job-pool--ensure-workers-locked pool)
    (jobpond--condition-broadcast (job-pool--condition-variable pool)))
  nil)


;;;; -- Deadline Monitor --

(defun job-pool--deadlines-possible-locked-p (pool)
  "Return T when some job of POOL can carry a runtime deadline, else NIL.

The pool default enables deadlines for every job, and a single job admitted with
its own positive cap enables them for that job alone."
  (if (or (plusp (job-pool-maximum-runtime-milliseconds pool))
          (loop for job being the hash-values of (job-pool--jobs pool)
                thereis (plusp (job-maximum-runtime-milliseconds job))))
      t
      nil))

(defun job-pool--monitor-loop (pool)
  "Cancel running jobs of POOL whose runtime deadlines have elapsed.

Job deadlines are read under each job's own lock, and the collected pool jobs are
read under the pool lock, but the two locks are never held at the same time in
this direction, so the monitor cannot deadlock against terminal publication."
  (loop
    (let ((expired nil)
          (jobs nil))
      (with-lock-held ((job-pool--lock pool))
        (when (job-pool--shutdown-p pool)
          (return-from job-pool--monitor-loop nil))
        (setf jobs (job-pool--collect-jobs-locked pool)))
      (let ((now (get-internal-real-time)))
        (dolist (job jobs)
          (with-lock-held ((job--lock job))
            (when (and (eq (job-state job) :running)
                       (job-deadline job)
                       (>= now (job-deadline job)))
              (push job expired)))))
      (dolist (job expired)
        (job-cancel job :reason :timeout))
      (with-lock-held ((job-pool--lock pool))
        (unless (job-pool--shutdown-p pool)
          (condition-wait (job-pool--condition-variable pool)
                          (job-pool--lock pool)
                          :timeout *monitor-poll-seconds*))))))

(defun job-pool--ensure-monitor-locked (pool)
  "Start POOL's deadline monitor when needed while its lock is held."
  (let ((monitor (job-pool--monitor-thread pool)))
    (when (and (job-pool--deadlines-possible-locked-p pool)
               (eq (job-pool-lifecycle-state pool) :open)
               (not (and monitor (thread-alive-p monitor))))
      (setf (job-pool--monitor-thread pool)
            (make-thread
             (lambda () (job-pool--monitor-loop pool))
             :name (format nil "~A deadline monitor"
                           (job-pool-name pool))))))
  nil)

(defun job-pool--ensure-monitor (pool)
  "Start POOL's single deadline monitor when some job can carry a deadline."
  (with-lock-held ((job-pool--lock pool))
    (job-pool--ensure-monitor-locked pool))
  nil)


;;;; -- Pool Construction --

(defun make-job-pool
    (&key (name "cl-jobpond pool")
       (maximum-concurrency *default-maximum-concurrency*)
       (maximum-batch-size *default-maximum-batch-size*)
       (maximum-live-jobs *default-maximum-live-jobs*)
       (maximum-runtime-milliseconds *default-maximum-runtime-milliseconds*)
       (terminal-retention-limit *default-terminal-retention-limit*)
       (job-class 'job)
       (start-threads-p t))
  "Create an open job pool and return it.

MAXIMUM-CONCURRENCY workers start immediately, so a pool costs its threads from
creation rather than from first use. Pass START-THREADS-P NIL for a pool that
costs nothing until its first admission: submission starts the threads it needs
anyway, so a host whose sessions mostly never submit a job pays only for the ones
that do. It also keeps such a host single threaded, which matters to anything that
forks or saves its own image.

MAXIMUM-RUNTIME-MILLISECONDS is the default wall-clock cap applied to every
admitted job, where zero disables deadlines. Every limit is validated, so a bad
bound signals JOB-POOL-INVALID-LIMIT here instead of misbehaving later. Always
pair this with JOB-POOL-CLOSE.

JOB-CLASS names the class this pool instantiates for each job, and must be JOB
or a subclass of it. A host with fields of its own subclasses JOB and passes
:INITARGS in each admission entry, which keeps those fields in real slots with
real accessors instead of inside the payload."
  (check-type name string)
  (unless (and (symbolp job-class)
               (find-class job-class nil)
               (subtypep job-class 'job))
    (error 'job-pool-invalid-limit
           :message ":JOB-CLASS must name JOB or a subclass of it."
           :limit-kind :job-class
           :value job-class))
  (let ((pool (make-instance
               'job-pool
               :name name
               :job-class job-class
               :maximum-concurrency
               (job-pool--validate-limit :maximum-concurrency
                                         maximum-concurrency
                                         :maximum *maximum-concurrency-limit*)
               :maximum-batch-size
               (job-pool--validate-limit :maximum-batch-size maximum-batch-size)
               :maximum-live-jobs
               (job-pool--validate-limit :maximum-live-jobs maximum-live-jobs)
               :maximum-runtime-milliseconds
               (job-pool--validate-limit :maximum-runtime-milliseconds
                                         maximum-runtime-milliseconds
                                         :minimum 0)
               :terminal-retention-limit
               (job-pool--validate-limit :terminal-retention-limit
                                         terminal-retention-limit))))
    (when start-threads-p
      (job-pool--ensure-workers pool)
      (job-pool--ensure-monitor pool))
    pool))


;;;; -- Pool Policy Updates --

(defun job-pool-update-limits
    (pool &key maximum-concurrency maximum-batch-size maximum-live-jobs
               maximum-runtime-milliseconds)
  "Atomically replace POOL's mutable limits and return POOL.

Every keyword is required. All four values are validated before the pool lock is
taken, so JOB-POOL-INVALID-LIMIT leaves the complete previous policy in place.
The new tuple is published under one lock acquisition, so lock-mediated
submission and worker claims cannot observe a partial policy. The individual
limit accessors remain unsynchronized inspection and do not form an atomic
snapshot when called concurrently with this operation. An open pool starts any
newly required workers and deadline monitor immediately, while lowering
concurrency leaves excess workers alive but unable to claim beyond the new bound.
A closing or closed pool keeps the new policy for its next refresh."
  (let ((validated-maximum-concurrency
          (job-pool--validate-limit :maximum-concurrency
                                    maximum-concurrency
                                    :maximum *maximum-concurrency-limit*))
        (validated-maximum-batch-size
          (job-pool--validate-limit :maximum-batch-size maximum-batch-size))
        (validated-maximum-live-jobs
          (job-pool--validate-limit :maximum-live-jobs maximum-live-jobs))
        (validated-maximum-runtime-milliseconds
          (job-pool--validate-limit :maximum-runtime-milliseconds
                                    maximum-runtime-milliseconds
                                    :minimum 0)))
    (with-lock-held ((job-pool--lock pool))
      (setf (job-pool-maximum-concurrency pool)
            validated-maximum-concurrency
            (job-pool-maximum-batch-size pool)
            validated-maximum-batch-size
            (job-pool-maximum-live-jobs pool)
            validated-maximum-live-jobs
            (job-pool-maximum-runtime-milliseconds pool)
            validated-maximum-runtime-milliseconds)
      (job-pool--ensure-workers-locked pool)
      (job-pool--ensure-monitor-locked pool)
      (jobpond--condition-broadcast (job-pool--condition-variable pool))))
  pool)


;;;; -- Pool Lifecycle --

(defun job-pool--reap-dead-threads-locked (pool)
  "Forget POOL's dead runtime threads and finish an ownerless shutdown.

A close owner that died before finishing leaves the pool :CLOSING forever, so its
ownership is released here and the pool becomes :CLOSED once no thread remains."
  (let ((owner (job-pool--close-owner pool)))
    (when (and owner (not (thread-alive-p owner)))
      (setf (job-pool--close-owner pool) nil)))
  (setf (job-pool--worker-threads pool)
        (remove-if-not #'thread-alive-p (job-pool--worker-threads pool)))
  (let ((monitor (job-pool--monitor-thread pool)))
    (when (and monitor (not (thread-alive-p monitor)))
      (setf (job-pool--monitor-thread pool) nil)))
  (when (and (eq (job-pool-lifecycle-state pool) :closing)
             (null (job-pool--close-owner pool))
             (null (job-pool--worker-threads pool))
             (null (job-pool--monitor-thread pool)))
    (setf (job-pool-lifecycle-state pool) :closed
          (job-pool--active-count pool) 0)
    (jobpond--condition-broadcast (job-pool--condition-variable pool)))
  nil)

(defun job-pool-refresh (pool)
  "Reopen a closed POOL, restart its reusable threads, and return POOL.

A pool that is still closing cannot be reopened and signals JOB-POOL-CLOSED,
because its previous workers may still be unwinding cancelled jobs."
  (with-lock-held ((job-pool--lock pool))
    (job-pool--reap-dead-threads-locked pool)
    (when (eq (job-pool-lifecycle-state pool) :closing)
      (error 'job-pool-closed
             :message (format nil "Pool ~A is still shutting down."
                              (job-pool-name pool))
             :lifecycle-state :closing))
    (when (eq (job-pool-lifecycle-state pool) :closed)
      (setf (job-pool-lifecycle-state pool) :open))
    (setf (job-pool--shutdown-p pool) nil)
    (jobpond--condition-broadcast (job-pool--condition-variable pool)))
  (job-pool--ensure-workers pool)
  (job-pool--ensure-monitor pool)
  pool)

(defun job-pool--await-close-owner (pool deadline)
  "Wait until POOL leaves :CLOSING or DEADLINE passes, then report closure.

A thread that did not win the close ownership waits here instead of racing the
owner through the same shutdown, so concurrent close calls agree on one answer."
  (with-lock-held ((job-pool--lock pool))
    (loop
      (let ((now (get-internal-real-time)))
        (when (or (not (eq (job-pool-lifecycle-state pool) :closing))
                  (null (job-pool--close-owner pool))
                  (>= now deadline))
          (return))
        (condition-wait (job-pool--condition-variable pool)
                        (job-pool--lock pool)
                        :timeout (/ (max 0 (- deadline now))
                                    internal-time-units-per-second))))
    (eq (job-pool-lifecycle-state pool) :closed)))

(defun job-pool--live-threads (threads)
  "Return the members of THREADS that are alive and are not the calling thread."
  (remove-if-not
   (lambda (thread)
     (and (not (eq thread (current-thread)))
          (thread-alive-p thread)))
   threads))

(defun job-pool-close (pool)
  "Cancel every job of POOL, stop its threads, and report complete shutdown.

Return T only when the pool reached :CLOSED, which means no worker and no monitor
thread of this pool is still running. Return NIL when a thread outlived
*SHUTDOWN-TIMEOUT-SECONDS*, in which case the pool stays :CLOSING and a later
call may finish the job.

Closing is graceful in the sense that every admitted job becomes terminal and
every waiter is woken: queued jobs publish :ABORTED, running jobs are interrupted
and publish their own terminal state as they unwind, and worker cleanup forms run
normally. It is not a drain: a running job is stopped rather than finished. Wait
for the jobs that matter with JOB-AWAIT before closing.

Closing is idempotent and safe from several threads at once. One caller becomes
the close owner and the others wait for its result."
  (let ((owner-p nil)
        (jobs nil)
        (threads nil)
        (deadline (+ (get-internal-real-time)
                     (* *shutdown-timeout-seconds*
                        internal-time-units-per-second))))
    (with-lock-held ((job-pool--lock pool))
      (job-pool--reap-dead-threads-locked pool)
      (case (job-pool-lifecycle-state pool)
        (:closed
         (return-from job-pool-close t))
        (:closing
         (let ((owner (job-pool--close-owner pool)))
           (unless (and owner
                        (not (eq owner (current-thread)))
                        (thread-alive-p owner))
             (setf owner-p t
                   (job-pool--close-owner pool) (current-thread)
                   jobs (job-pool--collect-jobs-locked pool)
                   threads (job-pool--collect-threads-locked pool)))))
        (otherwise
         (setf owner-p t
               (job-pool--close-owner pool) (current-thread)
               (job-pool-lifecycle-state pool) :closing
               (job-pool--shutdown-p pool) t
               (job-pool--queue pool) nil
               jobs (job-pool--collect-jobs-locked pool)
               threads (job-pool--collect-threads-locked pool))
         (jobpond--condition-broadcast (job-pool--condition-variable pool)))))
    (unless owner-p
      (return-from job-pool-close (job-pool--await-close-owner pool deadline)))
    (dolist (job jobs)
      (job-cancel job :reason :shutdown))
    (loop
      (let ((live (job-pool--live-threads threads)))
        (when (or (null live) (>= (get-internal-real-time) deadline))
          (return))
        (sleep 0.01)))
    ;; Only threads that have already finished are joined, so a thread stuck past
    ;; the deadline cannot make this call block indefinitely.
    (dolist (thread threads)
      (when (and (not (eq thread (current-thread)))
                 (not (thread-alive-p thread)))
        (join-thread thread)))
    (let ((live (job-pool--live-threads threads)))
      (with-lock-held ((job-pool--lock pool))
        (setf (job-pool--worker-threads pool)
              (intersection live (job-pool--worker-threads pool) :test #'eq)
              (job-pool--monitor-thread pool)
              (if (member (job-pool--monitor-thread pool) live :test #'eq)
                  (job-pool--monitor-thread pool)
                  nil)
              (job-pool-lifecycle-state pool) (if live :closing :closed)
              (job-pool--close-owner pool) nil
              (job-pool--active-count pool)
              (if live (job-pool--active-count pool) 0))
        (jobpond--condition-broadcast (job-pool--condition-variable pool)))
      (if live
          nil
          t))))

(defun job-pool-detach (pool)
  "Drop the runtime state of a closed POOL and return NIL.

This releases queued jobs, retained terminal jobs, and listeners so a closed pool
can be saved in an image or discarded without keeping its history alive. A pool
that is not closed, or that still owns a live thread, signals
JOB-POOL-DETACH-REFUSED rather than losing track of running work."
  (with-lock-held ((job-pool--lock pool))
    (unless (eq (job-pool-lifecycle-state pool) :closed)
      (error 'job-pool-detach-refused
             :message (format nil "Pool ~A must close before it can detach."
                              (job-pool-name pool))
             :reason :not-closed))
    (when (or (some #'thread-alive-p (job-pool--worker-threads pool))
              (let ((monitor (job-pool--monitor-thread pool)))
                (and monitor (thread-alive-p monitor))))
      (error 'job-pool-detach-refused
             :message (format nil "Pool ~A cannot detach while its threads live."
                              (job-pool-name pool))
             :reason :threads-alive))
    (setf (job-pool--worker-threads pool) nil
          (job-pool--monitor-thread pool) nil
          (job-pool--queue pool) nil
          (job-pool--close-owner pool) nil
          (job-pool--active-count pool) 0
          (job-pool--live-count pool) 0
          (job-pool--terminal-identifiers pool) nil
          (job-pool--listeners pool) nil)
    (clrhash (job-pool--jobs pool)))
  nil)
