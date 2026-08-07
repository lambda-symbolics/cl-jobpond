(in-package #:cl-jobpond)

;;;; -- Progress Reporting --

(defun job--set-progress-state (job state)
  "Mirror STATE into JOB's progress record and return NIL."
  (let ((progress (job--progress job)))
    (with-lock-held ((job-progress--lock progress))
      (setf (job-progress--state progress) state
            (job-progress--updated-at progress) (get-internal-real-time))
      (when (eq state :running)
        (setf (job-progress--started-at progress) (get-internal-real-time)))))
  nil)

(defun job--compact-progress (job state)
  "Make JOB's progress terminal under STATE and release its large fields.

Terminal jobs stay in the retention ring long after they finish, so the streamed
output tail shrinks to *RETAINED-PROGRESS-OUTPUT-LIMIT* characters here."
  (let ((progress (job--progress job)))
    (with-lock-held ((job-progress--lock progress))
      (let* ((output (job-progress--output-tail progress))
             (start (max 0 (- (length output) *retained-progress-output-limit*))))
        (setf (job-progress--state progress) state
              (job-progress--output-tail progress) (subseq output start)
              (job-progress--updated-at progress) (get-internal-real-time)))))
  nil)

(defun job--append-progress-output (progress text)
  "Append TEXT to PROGRESS while retaining only a bounded tail."
  (let* ((combined (concatenate 'string
                                (job-progress--output-tail progress)
                                text))
         (start (max 0 (- (length combined) *progress-output-limit*))))
    (setf (job-progress--output-tail progress) (subseq combined start)))
  nil)

(defun job-cancellation-requested-p (job)
  "Return T when a controller has requested cancellation of JOB, else NIL."
  (if (with-lock-held ((job--lock job))
        (job-cancellation-reason job))
      t
      nil))

(defun job-check-cancellation (job)
  "Signal JOB-ABORTED when cancellation is pending for JOB, else return NIL.

A job body that cannot be interrupted safely calls this at its own safe points.
A job that has already published a terminal result is never aborted again."
  (let ((reason (with-lock-held ((job--lock job))
                  (if (job--terminal-state-p (job-state job))
                      nil
                      (job-cancellation-reason job)))))
    (when reason
      (error 'job-aborted
             :message (format nil "Job ~A was ~(~A~)."
                              (job-identifier job) reason)
             :identifier (job-identifier job)
             :reason reason))
    nil))

(defun job-report-progress (job &key (detail nil detail-supplied-p) output
                                  (steps 1))
  "Record one progress report for JOB, then check for pending cancellation.

DETAIL replaces the newest progress value when supplied, OUTPUT is appended to
the bounded output tail, and STEPS increments the reported step count. The
report is published to POOL listeners on the :JOB-PROGRESS channel.

Every report is also a cancellation point: this signals JOB-ABORTED when a
controller requested cancellation, so a cooperative body that reports progress
regularly never needs an interrupt to stop."
  (let ((progress (job--progress job))
        (event nil))
    (with-lock-held ((job-progress--lock progress))
      (when detail-supplied-p
        (setf (job-progress--detail progress) detail))
      (when output
        (job--append-progress-output progress output))
      (incf (job-progress--step-count progress) steps)
      (setf (job-progress--updated-at progress) (get-internal-real-time)
            event (list :job job
                        :identifier (job-identifier job)
                        :index (job-index job)
                        :status (job-progress--state progress)
                        :step-count (job-progress--step-count progress)
                        :detail (job-progress--detail progress))))
    (job-pool-emit (job-pool job) :job-progress event))
  (job-check-cancellation job))


;;;; -- Snapshots --

(defun job--progress-snapshot (job &key ended-at)
  "Return JOB's progress plist, timing its duration against ENDED-AT.

The caller holds JOB's lifecycle lock and supplies ENDED-AT from it, so a
snapshot never mixes a running duration with a terminal state."
  (let ((progress (job--progress job)))
    (with-lock-held ((job-progress--lock progress))
      (list :identifier (job-identifier job)
            :index (job-index job)
            :status (job-progress--state progress)
            :detail (job-progress--detail progress)
            :recent-output (job-progress--output-tail progress)
            :step-count (job-progress--step-count progress)
            :duration-milliseconds
            (if (job-progress--started-at progress)
                (jobpond--milliseconds-between
                 (job-progress--started-at progress)
                 (or ended-at (get-internal-real-time)))
                nil)))))

(defun job-progress-snapshot (job)
  "Return a coherent portable plist describing JOB's current progress."
  (with-lock-held ((job--lock job))
    (job--progress-snapshot job :ended-at (job-ended-at job))))

(defun job--snapshot-locked (job)
  "Return JOB's portable snapshot while its lifecycle lock is held."
  (list :identifier (job-identifier job)
        :index (job-index job)
        :name (job-name job)
        :state (job-state job)
        :result (job-result job)
        :cancellation-reason (job-cancellation-reason job)
        :condition-report (job-condition-report job)
        :created-at (job-created-at job)
        :started-at (job-started-at job)
        :ended-at (job-ended-at job)
        :progress (job--progress-snapshot job :ended-at (job-ended-at job))))

(defun job-snapshot (job)
  "Return JOB's coherent portable lifecycle, result, and progress snapshot."
  (with-lock-held ((job--lock job))
    (job--snapshot-locked job)))

(defun job-terminal-p (job)
  "Return T when JOB can make no further state transition, else NIL."
  (with-lock-held ((job--lock job))
    (job--terminal-state-p (job-state job))))


;;;; -- Waiting --

(defun job-await (job &key timeout-seconds)
  "Wait for JOB to become terminal and return its snapshot and a terminal flag.

TIMEOUT-SECONDS bounds the wait; NIL waits indefinitely. The second value is T
only when JOB is terminal, so a caller distinguishes a result from a timeout.
The wait is driven by JOB's condition variable, which terminal publication
broadcasts, so no polling is involved."
  (let ((deadline (if timeout-seconds
                      (+ (get-internal-real-time)
                         (* timeout-seconds internal-time-units-per-second))
                      nil)))
    (with-lock-held ((job--lock job))
      (loop
        (when (job--terminal-state-p (job-state job))
          (return))
        (let ((remaining (if deadline
                             (/ (max 0 (- deadline (get-internal-real-time)))
                                internal-time-units-per-second)
                             nil)))
          (when (and remaining (<= remaining 0))
            (return))
          (condition-wait (job--condition-variable job)
                          (job--lock job)
                          :timeout remaining)))))
  (let ((snapshot (job-snapshot job)))
    (values snapshot (job--terminal-state-p (getf snapshot :state)))))
