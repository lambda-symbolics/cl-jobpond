(in-package #:cl-jobpond/tests)

;;;; -- Completion Delivery Tests --

(defun completion-tests--snapshot (id state)
  "Return a complete persisted snapshot fixture."
  (list :identifier id :index 1 :name "fixture" :state state :result nil
        :cancellation-reason nil :condition-report nil :created-at 0
        :started-at nil :ended-at nil :progress nil))

(defun completion-tests--messages (subscription)
  "Inspect pending delivery state without claiming it."
  (getf (cl-jobpond:completion-subscription-snapshot subscription) :messages))

(defun completion-tests--restore ()
  "Exercise deduplication and restart on both sides of the delivery boundary."
  (let* ((persisted nil)
         (subscription (cl-jobpond:make-completion-subscription
                        :store (lambda (state)
                                 (setf persisted (coordination--roundtrip state)))))
         (snapshot (completion-tests--snapshot "local-1" ':completed)))
    (cl-jobpond:completion-subscription-replay subscription snapshot :id "execution-uuid")
    (coordination--check
     (nth-value 1 (cl-jobpond:completion-subscription-replay subscription snapshot
                                                           :id "execution-uuid")))
    (coordination--check (= 1 (length (completion-tests--messages subscription))))
    (let* ((restored (cl-jobpond:make-completion-subscription :snapshot persisted))
           (record (first (cl-jobpond:completion-subscription-collect restored))))
      (coordination--check (equal "execution-uuid" (getf record :id)))
      (coordination--check (eq ':success (getf (getf record :payload) :outcome)))
      (coordination--check
       (equal "local-1" (getf (getf (getf record :payload) :snapshot) :identifier)))
      (coordination--check (null (cl-jobpond:completion-subscription-collect restored)))
      (let* ((claimed (coordination--roundtrip
                       (cl-jobpond:completion-subscription-snapshot restored)))
             (uncertain (cl-jobpond:make-completion-subscription :snapshot claimed))
             (unknown (first (completion-tests--messages uncertain))))
        (coordination--check (eq ':unknown (getf unknown :state)))
        (coordination--check (eq ':success (getf (getf unknown :payload) :outcome)))
        (coordination--check (null (cl-jobpond:completion-subscription-collect uncertain)))
        (cl-jobpond:mailbox-resolve (cl-jobpond:completion-subscription-mailbox uncertain)
                                  :id (getf record :id) :receiver "completion"
                                  :token (getf record :token) :action ':retry)
        (let ((retry (first (cl-jobpond:completion-subscription-collect uncertain))))
          (coordination--check (> (getf retry :token) (getf record :token)))
          (coordination--fails ':stale-token
            (cl-jobpond:completion-subscription-ack uncertain :id (getf retry :id)
                                                   :token (getf record :token)))
          (cl-jobpond:completion-subscription-ack uncertain :id (getf retry :id)
                                                 :token (getf retry :token)
                                                 :result '(:receipt "conversation-42"))
          (let ((delivered (cl-jobpond:make-completion-subscription
                            :snapshot (cl-jobpond:completion-subscription-snapshot uncertain))))
            (coordination--check
             (nth-value 1 (cl-jobpond:completion-subscription-replay delivered snapshot
                                                                  :id "execution-uuid")))
            (coordination--check (null (cl-jobpond:completion-subscription-collect delivered)))))))
    (cl-jobpond:completion-subscription-replay subscription
                                             (completion-tests--snapshot "interrupted" ':running))
    (coordination--check
     (eq ':unknown (getf (getf (second (completion-tests--messages subscription)) :payload)
                        :outcome)))
    (coordination--fails ':identity-conflict
      (cl-jobpond:completion-subscription-replay subscription
                                               (completion-tests--snapshot "local-1" ':failed)
                                               :id "execution-uuid"))
    (coordination--check (cl-jobpond:completion-subscription-close subscription))
    (coordination--check (null (cl-jobpond:completion-subscription-close subscription)))
    (coordination--check (= 2 (length (cl-jobpond:completion-subscription-collect subscription))))
    (coordination--fails ':closed
      (cl-jobpond:completion-subscription-replay subscription snapshot))))

(defun completion-tests--publication ()
  "Exercise preadmission attachment, concurrent production and bounded batching."
  (let* ((pool (make-job-pool :maximum-concurrency 4 :maximum-live-jobs 32
                             :maximum-batch-size 32))
         (gate (tests--make-gate))
         (wake (tests--make-gate))
         (subscription nil)
         (jobs nil))
    (setf subscription
          (cl-jobpond:make-completion-subscription
           :capacity 32
           :identity-function (lambda (job snapshot)
                                (declare (ignore snapshot)) (job-payload job))
           :snapshot-function
           (lambda (job snapshot)
             ;; Reentrant inspection proves that neither pool/job nor subscription
             ;; locks are held while the caller converts the complete snapshot.
             (job-snapshot job)
             (job-pool-list-jobs pool)
             (cl-jobpond:completion-subscription-snapshot subscription)
             snapshot)
           :wakeup (lambda (subscription)
                     (cl-jobpond:completion-subscription-snapshot subscription)
                     (tests--gate-open wake))))
    (unwind-protect
         (progn
           (cl-jobpond:completion-subscription-attach subscription pool)
           (setf jobs
                 (job-pool-submit-batch
                  pool (loop for index below 24
                             collect (list :payload (format nil "uuid-~D" index)
                                           :function (lambda (job)
                                                       (tests--gate-await gate)
                                                       (job-payload job))))))
           (tests--gate-open gate)
           (coordination--check (tests--gate-await wake))
           (dolist (job jobs) (job-await job :timeout-seconds 10))
           (job-pool-close pool)
           (coordination--check (= 24 (length (completion-tests--messages subscription))))
           (let ((batch (cl-jobpond:completion-subscription-collect subscription :limit 7)))
             (coordination--check (= 7 (length batch)))
             (coordination--check (= 7 (length (remove-duplicates batch :key
                                                                (lambda (record) (getf record :id))
                                                                :test #'equal)))))
           (coordination--check (= 17 (length
                                      (cl-jobpond:completion-subscription-collect subscription
                                                                                :limit 32))))
           (coordination--check (null (cl-jobpond:completion-subscription-collect subscription)))
           ;; Replay even an evicted terminal object and race duplicate watches.
           (let ((threads (loop repeat 8
                                collect (make-thread
                                         (lambda ()
                                           (cl-jobpond:completion-subscription-watch
                                            subscription (first jobs)))))))
             (dolist (thread threads) (join-thread thread)))
           (coordination--check (= 24 (length (completion-tests--messages subscription)))))
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool))))

(defun completion-tests--failures ()
  "Retain truthful failures/cancellations and recover producer callback refusals."
  (let* ((pool (make-job-pool :maximum-concurrency 1 :terminal-retention-limit 1))
         (fail t) (persisted nil)
         (subscription
           (cl-jobpond:make-completion-subscription
            :store (lambda (state)
                     (when fail (error "Atomic store refused"))
                     (setf persisted (coordination--roundtrip state)))
            :wakeup (lambda (subscription)
                      (declare (ignore subscription)) (error "Wakeup refused"))))
         (job (job-pool-submit pool (lambda (job) (declare (ignore job)) (error "Body failed")))))
    (unwind-protect
         (progn
           (handler-case (cl-jobpond:completion-subscription-watch subscription job)
             (error () nil))
           (job-await job :timeout-seconds 10)
           ;; Close joins the worker, including its terminal producer callback.
           (job-pool-close pool)
           (coordination--check (stringp (cl-jobpond:completion-subscription-error subscription)))
           (coordination--check (null persisted))
           (setf fail nil)
           (coordination--check (= 1 (cl-jobpond:completion-subscription-refresh subscription)))
           (let* ((record (first (cl-jobpond:completion-subscription-collect subscription)))
                  (event (getf record :payload)))
             (coordination--check (eq ':failure (getf event :outcome)))
             (coordination--check (search "Body failed"
                                          (getf (getf event :snapshot) :condition-report))))
           (coordination--check (stringp (cl-jobpond:completion-subscription-error subscription))))
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool)))
  (let* ((pool (make-job-pool :maximum-concurrency 1))
         (subscription (cl-jobpond:make-completion-subscription))
         (job (job-pool-submit pool (lambda (job) (declare (ignore job)) "never")
                              :inline-only-p t)))
    (unwind-protect
         (progn
           (cl-jobpond:completion-subscription-watch subscription job :id "cancelled-uuid")
           (job-cancel job :reason ':requested)
           (let ((event (getf (first (cl-jobpond:completion-subscription-collect subscription))
                             :payload)))
             (coordination--check (eq ':cancelled (getf event :outcome)))
             (coordination--check (eq ':requested (getf (getf event :snapshot)
                                                      :cancellation-reason)))))
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool))))

(defun completion-tests--bounds ()
  "Refuse overflow without dropping existing events or deduplication proof."
  (let ((subscription (cl-jobpond:make-completion-subscription :capacity 1 :history-limit 1)))
    (cl-jobpond:completion-subscription-replay subscription
                                             (completion-tests--snapshot "one" ':completed))
    (coordination--fails ':capacity
      (cl-jobpond:completion-subscription-replay subscription
                                               (completion-tests--snapshot "two" ':completed)))
    (let ((record (first (cl-jobpond:completion-subscription-collect subscription))))
      (cl-jobpond:completion-subscription-ack subscription :id "one" :token (getf record :token)))
    (coordination--fails ':capacity
      (cl-jobpond:completion-subscription-replay subscription
                                               (completion-tests--snapshot "two" ':completed)))
    (coordination--check
     (nth-value 1 (cl-jobpond:completion-subscription-replay subscription
                                                           (completion-tests--snapshot "one" ':completed))))
    (coordination--check (= 1 (length (completion-tests--messages subscription))))
    (cl-jobpond:completion-subscription-close subscription)))

(defun completion-tests--attachment ()
  "Exercise running/evicted attachment, projection retry and listener closure."
  (let* ((pool (make-job-pool :maximum-concurrency 1 :terminal-retention-limit 1))
         (gate (tests--make-gate)) (entered (tests--make-gate))
         (fail t) (wake-count 0)
         (subscription
           (cl-jobpond:make-completion-subscription
            :snapshot-function (lambda (job snapshot)
                                 (declare (ignore job))
                                 (when fail (error "Projection refused")) snapshot)
            :wakeup (lambda (subscription)
                      (declare (ignore subscription)) (incf wake-count))))
         (job (job-pool-submit pool
                               (lambda (job)
                                 (declare (ignore job))
                                 (tests--gate-open entered)
                                 (tests--gate-await gate) "complete"))))
    (unwind-protect
         (progn
           (coordination--check (tests--gate-await entered))
           (cl-jobpond:completion-subscription-watch subscription job :id "durable-job")
           (tests--gate-open gate)
           (job-await job :timeout-seconds 10)
           (job-pool-close pool)
           (coordination--check (stringp (cl-jobpond:completion-subscription-error subscription)))
           (coordination--check (null (completion-tests--messages subscription)))
           (setf fail nil)
           (job-pool-refresh pool)
           (job-await (job-pool-submit pool (lambda (job) (declare (ignore job)) "evict"))
                      :timeout-seconds 10)
           (job-pool-close pool)
           ;; The original job left pool lookup but its retained watch still yields
           ;; its full outcome on a successful retry.
           (coordination--check
            (handler-case (progn (job-pool-find-job pool (job-identifier job)) nil)
              (job-not-found () t)))
           (cl-jobpond:completion-subscription-refresh subscription)
           (coordination--check (= 1 (length (completion-tests--messages subscription))))
           (coordination--check
            (equal "complete" (getf (getf (getf (first (completion-tests--messages subscription))
                                                :payload) :snapshot) :result)))
           (let ((late (cl-jobpond:make-completion-subscription)))
             (unwind-protect
                  (progn
                    (cl-jobpond:completion-subscription-watch late job :id "late-uuid")
                    (coordination--check (= 1 (length (completion-tests--messages late)))))
               (cl-jobpond:completion-subscription-close late)))
           (coordination--fails ':already-attached
             (cl-jobpond:completion-subscription-attach subscription pool))
           (cl-jobpond:completion-subscription-close subscription)
           (let ((before wake-count))
             (job-pool-refresh pool)
             (job-await (job-pool-submit pool (lambda (job) (declare (ignore job)) "after-close"))
                        :timeout-seconds 10)
             (job-pool-close pool)
             (coordination--check (= before wake-count))))
      (tests--gate-open gate)
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool))))

(defun completion-tests--forget ()
  "Deliver beyond bounded history while preserving watches and in-flight claims."
  (let* ((pool (make-job-pool :maximum-concurrency 1))
         (gate (tests--make-gate))
         (receipts (make-hash-table :test 'equal))
         (subscription (cl-jobpond:make-completion-subscription
                        :capacity 4 :history-limit 4
                        :identity-function (lambda (job snapshot)
                                             (declare (ignore snapshot))
                                             (unless (gethash (job-identifier job) receipts)
                                               (job-identifier job))))))
    (unwind-protect
         (let ((job (job-pool-submit pool (lambda (job)
                                          (declare (ignore job))
                                          (tests--gate-await gate) "watched"))))
           (cl-jobpond:completion-subscription-watch subscription job)
           (cl-jobpond:completion-subscription-replay
            subscription (completion-tests--snapshot "retained" ':failed))
           (let ((retained (first (cl-jobpond:completion-subscription-collect subscription))))
             (dotimes (index 1100)
               (let ((id (format nil "event-~D" index)))
                 (cl-jobpond:completion-subscription-replay
                  subscription (completion-tests--snapshot id ':completed))
                 (let ((record (first (cl-jobpond:completion-subscription-collect subscription))))
                   (coordination--check (equal id (getf record :id)))
                   (setf (gethash id receipts) t)
                   (cl-jobpond:completion-subscription-ack subscription :id id
                                                         :token (getf record :token))
                   (coordination--check
                    (= 1 (cl-jobpond:completion-subscription-forget subscription :ids (list id)))))))
             (coordination--check (equal (list retained) (completion-tests--messages subscription)))
             (tests--gate-open gate)
             (job-await job :timeout-seconds 10)
             (let ((record (first (cl-jobpond:completion-subscription-collect subscription))))
               (coordination--check (equal (job-identifier job) (getf record :id)))
               (coordination--check (equal "watched" (getf (getf (getf record :payload) :snapshot) :result)))
               (setf (gethash (getf record :id) receipts) t)
               (cl-jobpond:completion-subscription-ack subscription :id (getf record :id)
                                                     :token (getf record :token))
               (cl-jobpond:completion-subscription-forget subscription :ids (list (getf record :id)))
               (cl-jobpond:completion-subscription-watch subscription job)
               (coordination--check (equal (list retained) (completion-tests--messages subscription))))
             (cl-jobpond:completion-subscription-ack subscription :id "retained"
                                                   :token (getf retained :token))
             (cl-jobpond:completion-subscription-forget subscription :ids '("retained"))
             (coordination--check (null (completion-tests--messages subscription)))))
      (tests--gate-open gate)
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool))))

(defun completion-tests--retired-refresh ()
  "Do not republish a retired watch from an in-flight concurrent conversion."
  (let* ((pool (make-job-pool :maximum-concurrency 1))
         (body-gate (tests--make-gate))
         (conversion-entered (tests--make-gate))
         (conversion-release (tests--make-gate))
         (lock (make-lock "Conversion order"))
         (conversions 0)
         (subscription
           (cl-jobpond:make-completion-subscription
            :snapshot-function
            (lambda (job snapshot)
              (declare (ignore job))
              (when (= 1 (with-lock-held (lock) (incf conversions)))
                (tests--gate-open conversion-entered)
                (tests--gate-await conversion-release))
              snapshot))))
    (unwind-protect
         (let ((job (job-pool-submit pool
                                    (lambda (job)
                                      (declare (ignore job))
                                      (tests--gate-await body-gate)
                                      "completed"))))
           (cl-jobpond:completion-subscription-watch subscription job)
           (tests--gate-open body-gate)
           (coordination--check (tests--gate-await conversion-entered))
           (coordination--check (= 1 (cl-jobpond:completion-subscription-refresh subscription)))
           (let ((record (first (cl-jobpond:completion-subscription-collect subscription))))
             (coordination--check (equal "completed"
                                         (getf (getf (getf record :payload) :snapshot) :result)))
             (cl-jobpond:completion-subscription-ack subscription :id (getf record :id)
                                                   :token (getf record :token))
             (coordination--check
              (= 1 (cl-jobpond:completion-subscription-forget subscription
                                                              :ids (list (getf record :id))))))
           (tests--gate-open conversion-release)
           (job-pool-close pool)
           (coordination--check (null (completion-tests--messages subscription)))
           (coordination--check (null (cl-jobpond:completion-subscription-error subscription))))
      (tests--gate-open body-gate)
      (tests--gate-open conversion-release)
      (cl-jobpond:completion-subscription-close subscription)
      (job-pool-close pool))))

(defun run-completion-tests ()
  "Run completion publication, concurrency, restore and failure checks."
  (let ((*coordination-checks* 0))
    (completion-tests--restore)
    (completion-tests--publication)
    (completion-tests--failures)
    (completion-tests--bounds)
    (completion-tests--attachment)
    (completion-tests--forget)
    (completion-tests--retired-refresh)
    (format t "~&~D completion assertions passed.~%" *coordination-checks*))
  nil)
