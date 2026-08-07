(in-package #:cl-jobpond)

;;;; -- Entry Validation --

(defun job-pool--entry-function (entry)
  "Return ENTRY's job body or signal JOB-POOL-INVALID-ENTRY."
  (let ((function (and (listp entry) (getf entry :function))))
    (if (functionp function)
        function
        (error 'job-pool-invalid-entry
               :message "A submission entry needs a :FUNCTION of one argument."
               :entry entry))))

(defun job-pool--entry-runtime (pool entry)
  "Return the wall-clock cap ENTRY requests, defaulting to POOL's own cap."
  (let ((requested (and (listp entry)
                        (getf entry :maximum-runtime-milliseconds))))
    (if requested
        (job-pool--validate-limit :maximum-runtime-milliseconds
                                  requested
                                  :minimum 0)
        (job-pool-maximum-runtime-milliseconds pool))))

(defun job-pool--entry-owner-identifiers (entry)
  "Return ENTRY's validated ancestor identifiers, outermost first."
  (let ((owners (getf entry :owner-identifiers)))
    (unless (and (listp owners)
                 (every (lambda (owner)
                          (and (stringp owner) (plusp (length owner))))
                        owners))
      (error 'job-pool-invalid-entry
             :message ":OWNER-IDENTIFIERS must be a list of non-empty strings."
             :entry entry))
    (copy-list owners)))

(defun job-pool--entry-root-identifier (entry)
  "Return ENTRY's validated tree identifier, or NIL when it names no tree."
  (let ((root (getf entry :root-identifier)))
    (unless (or (null root)
                (and (stringp root) (plusp (length root))))
      (error 'job-pool-invalid-entry
             :message ":ROOT-IDENTIFIER must be NIL or a non-empty string."
             :entry entry))
    root))

(defun job-pool--entry-initargs (entry)
  "Return ENTRY's validated extra initialization arguments for the job class."
  (let ((initargs (getf entry :initargs)))
    (unless (and (listp initargs) (evenp (length initargs)))
      (error 'job-pool-invalid-entry
             :message ":INITARGS must be a plist of initialization arguments."
             :entry entry))
    (copy-list initargs)))

(defun job-pool--entry-name (entry)
  "Return ENTRY's descriptive name or signal JOB-POOL-INVALID-ENTRY."
  (let ((name (and (listp entry) (getf entry :name))))
    (if (or (null name) (stringp name))
        name
        (error 'job-pool-invalid-entry
               :message "A submission entry :NAME must be a string or NIL."
               :entry entry))))


;;;; -- Atomic Admission --

(defun job-pool--create-job-locked (pool entry)
  "Create one job for ENTRY while POOL's lock is held.

The identifier ends in the admission index, so identifiers stay unique without
reserving names and terminal retention can evict a job without freeing a name."
  (incf (job-pool--next-index pool))
  (let* ((index (job-pool--next-index pool))
         (name (getf entry :name))
         (fragment (or (jobpond--identifier-fragment name) "job")))
    (apply #'make-instance
           (job-pool-job-class pool)
           :pool pool
           :identifier (format nil "~A-~D" fragment index)
           :index index
           :name name
           :payload (getf entry :payload)
           :owner-identifiers (getf entry :owner-identifiers)
           :root-identifier (getf entry :root-identifier)
           :body-function (getf entry :function)
           :maximum-runtime-milliseconds
           (getf entry :maximum-runtime-milliseconds)
           (getf entry :initargs))))

(defun job-pool-submit-batch (pool entries)
  "Admit every entry of ENTRIES into POOL atomically and return the new jobs.

Each entry is a plist. :FUNCTION is required and is called with the job as its
only argument. :NAME gives the job a readable identifier fragment, :PAYLOAD is
carried on the job for the body to read, and :MAXIMUM-RUNTIME-MILLISECONDS
overrides the pool wall-clock cap for that job alone. :INITARGS is a plist passed
on to the pool's job class, which is how a host subclassing JOB fills its own
slots.

:OWNER-IDENTIFIERS names this job's ancestors, outermost first, and
:ROOT-IDENTIFIER names the tree it belongs to. Both are what makes a cascading
cancellation possible: a job is a descendant of every identifier in its owner
list, so one pass over the pool finds a whole subtree.

Admission is all or nothing. Entries are validated and normalized before the pool
lock is taken, and the batch-size and live-job bounds are checked under that same
lock as the jobs enter the queue, so a refused batch admits nothing and a batch
that returns has every job queued. A refusal signals JOB-POOL-CAPACITY-EXCEEDED
or JOB-POOL-CLOSED and leaves the pool untouched."
  (check-type entries list)
  (let* ((normalized
           (mapcar (lambda (entry)
                     (list :function (job-pool--entry-function entry)
                           :name (job-pool--entry-name entry)
                           :payload (getf entry :payload)
                           :owner-identifiers
                           (job-pool--entry-owner-identifiers entry)
                           :root-identifier
                           (job-pool--entry-root-identifier entry)
                           :initargs (job-pool--entry-initargs entry)
                           :maximum-runtime-milliseconds
                           (job-pool--entry-runtime pool entry)))
                   entries))
         (count (length normalized))
         (jobs nil))
    (when (zerop count)
      (return-from job-pool-submit-batch nil))
    (with-lock-held ((job-pool--lock pool))
      (when (or (job-pool--shutdown-p pool)
                (not (eq (job-pool-lifecycle-state pool) :open)))
        (error 'job-pool-closed
               :message (format nil "Pool ~A is not accepting jobs."
                                (job-pool-name pool))
               :lifecycle-state (job-pool-lifecycle-state pool)))
      (when (> count (job-pool-maximum-batch-size pool))
        (error 'job-pool-capacity-exceeded
               :message (format nil "Pool ~A admits at most ~D jobs per batch."
                                (job-pool-name pool)
                                (job-pool-maximum-batch-size pool))
               :limit-kind :batch-size
               :limit (job-pool-maximum-batch-size pool)
               :requested-count count
               :live-count (job-pool--live-count pool)))
      (when (> (+ (job-pool--live-count pool) count)
               (job-pool-maximum-live-jobs pool))
        (error 'job-pool-capacity-exceeded
               :message (format nil "Pool ~A admits at most ~D live jobs."
                                (job-pool-name pool)
                                (job-pool-maximum-live-jobs pool))
               :limit-kind :live-jobs
               :limit (job-pool-maximum-live-jobs pool)
               :requested-count count
               :live-count (job-pool--live-count pool)))
      (setf jobs (mapcar (lambda (entry)
                           (job-pool--create-job-locked pool entry))
                         normalized)
            (job-pool--queue pool)
            (nconc (job-pool--queue pool) (copy-list jobs)))
      (dolist (job jobs)
        (setf (gethash (job-identifier job) (job-pool--jobs pool)) job))
      (incf (job-pool--live-count pool) count)
      (jobpond--condition-broadcast (job-pool--condition-variable pool)))
    (job-pool--ensure-workers pool)
    (job-pool--ensure-monitor pool)
    jobs))

(defun job-pool-submit (pool function &key name payload owner-identifiers
                                        root-identifier
                                        maximum-runtime-milliseconds)
  "Admit one job running FUNCTION into POOL and return the job.

FUNCTION is called with the job as its only argument. Returning a value publishes
:COMPLETED with that value, signalling JOB-ABORTED publishes :ABORTED, and
signalling any other error publishes :FAILED with a bounded condition report."
  (first (job-pool-submit-batch
          pool
          (list (list :function function
                      :name name
                      :payload payload
                      :owner-identifiers owner-identifiers
                      :root-identifier root-identifier
                      :maximum-runtime-milliseconds
                      maximum-runtime-milliseconds)))))


;;;; -- Inline Execution --

(defun job-run-inline (job)
  "Run queued JOB on the calling thread and return T when this call claimed it.

A caller that is about to block waiting for JOB can run it here instead, which
keeps a pool from deadlocking when every worker is occupied by a job waiting on
another job. The claim removes JOB from the pool queue under the pool lock, so a
reusable worker and an inline runner never run the same job twice. Return NIL
when JOB was already claimed, cancelled, or started."
  (let ((pool (job-pool job))
        (claimed-p nil))
    (with-lock-held ((job--lock job))
      (when (and (eq (job-state job) :queued)
                 (null (job-cancellation-reason job))
                 (not (job--publication-claimed-p job)))
        (with-lock-held ((job-pool--lock pool))
          (when (member job (job-pool--queue pool) :test #'eq)
            (setf (job-pool--queue pool)
                  (remove job (job-pool--queue pool) :test #'eq)
                  claimed-p t)
            (jobpond--condition-broadcast
             (job-pool--condition-variable pool))))))
    (when claimed-p
      (job--execute job))
    claimed-p))
