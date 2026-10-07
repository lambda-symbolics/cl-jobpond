(in-package #:cl-jobpond)

;;;; -- Completion Subscriptions --

(export '(completion-event completion-subscription make-completion-subscription
          completion-subscription-mailbox completion-subscription-error
          completion-subscription-attach completion-subscription-watch
          completion-subscription-refresh completion-subscription-replay
          completion-subscription-collect completion-subscription-ack
          completion-subscription-forget completion-subscription-snapshot
          completion-subscription-close))

(defun completion-event (snapshot &key id)
  "Copy a complete portable job SNAPSHOT into a stable terminal event.
Persisted nonterminal states reconstruct an UNKNOWN outcome, never success.
ID defaults to the job identifier. Supply a durable caller identity across restarts.
Opaque result/progress values require caller conversion before this function."
  (let* ((snapshot (durable-state--copy snapshot))
         (id (or id (getf snapshot :identifier)))
         (state (getf snapshot :state)))
    (durable-state--identifier id)
    (durable-state--plist snapshot
                          '(:identifier :index :name :state :result
                            :cancellation-reason :condition-report
                            :created-at :started-at :ended-at :progress))
    (unless (member state '(:queued :running :completed :failed :aborted :unknown))
      (durable-state--fail ':state id))
    (list :format 1 :id id :job-id id
          :outcome (case state (:completed :success) (:failed :failure)
                         (:aborted :cancelled) (otherwise :unknown))
          :snapshot snapshot)))

(defclass completion-subscription ()
  ((lock :initform (make-lock "Completion subscription")
         :reader completion-subscription--lock
         :documentation "Serializes watches, enqueue and closure.")
   (mailbox :initarg :mailbox :reader completion-subscription-mailbox
            :documentation "Acknowledged portable event storage and delivery history.")
   (capacity :initarg :capacity :reader completion-subscription--capacity
             :documentation "Maximum unresolved runtime watches.")
   (watches :initform nil :accessor completion-subscription--watches
            :documentation "Job references retained until successful persistence.")
   (listeners :initform nil :accessor completion-subscription--listeners
              :documentation "Pool and listener pairs to detach on closure.")
   (closed-p :initform nil :accessor completion-subscription--closed-p
             :documentation "True after detaching all producer listeners.")
   (wakeup :initarg :wakeup :reader completion-subscription--wakeup
           :documentation "Optional short function called outside all locks.")
   (identity-function :initarg :identity-function
                      :reader completion-subscription--identity-function
                      :documentation "Resolve a stable caller identity from JOB and its snapshot.")
   (snapshot-function :initarg :snapshot-function
                      :reader completion-subscription--snapshot-function
                      :documentation "Convert a coherent job snapshot to portable data.")
   (error :initform nil :accessor completion-subscription--error
          :documentation "Newest producer or wakeup failure report, or NIL."))
  (:documentation "Bounded completion subscription backed by an acknowledged mailbox."))

(defun make-completion-subscription (&key (capacity 64) (history-limit 4096)
                                         store snapshot wakeup
                                         (identity-function (lambda (job snapshot)
                                                              (declare (ignore snapshot))
                                                              (job-identifier job)))
                                         (snapshot-function (lambda (job snapshot)
                                                              (declare (ignore job))
                                                              snapshot)))
  "Create or restore a completion subscription.
STORE atomically replaces the mailbox snapshot before event visibility. It must
not reenter the subscription. A signalled error promises no external commit;
nonlocal exits require reconstruction from external storage. WAKEUP receives the
subscription outside locks on the producer thread and must only signal/schedule
short work. It may run concurrently, repeatedly, or after CLOSE has started.
IDENTITY-FUNCTION and SNAPSHOT-FUNCTION receive JOB and its coherent snapshot
outside all locks. Identity must be stable; snapshots must preserve outcome fields
and raw identifier. Callbacks must be idempotent under concurrent retry.
A NIL identity ignores the job.
Restored delivered claims are UNKNOWN delivery states, not unknown job outcomes;
use MAILBOX-RESOLVE explicitly to retry after checking caller delivery history."
  (unless (and (or (null wakeup) (functionp wakeup)) (functionp snapshot-function)
               (functionp identity-function))
    (durable-state--fail ':configuration))
  (let ((mailbox (make-mailbox :capacity capacity :history-limit history-limit
                               :store store :snapshot snapshot)))
    (make-instance 'completion-subscription :mailbox mailbox
                   :capacity (getf (mailbox-snapshot mailbox) :capacity)
                   :wakeup wakeup :snapshot-function snapshot-function
                   :identity-function identity-function)))

(defun completion-subscription-error (subscription)
  "Return the newest callback failure report as an isolated string, or NIL."
  (with-lock-held ((completion-subscription--lock subscription))
    (let ((report (completion-subscription--error subscription)))
      (when report (copy-seq report)))))

(defun completion-subscription--record-error (subscription condition)
  "Retain a bounded report without changing the job outcome."
  (with-lock-held ((completion-subscription--lock subscription))
    (setf (completion-subscription--error subscription)
          (jobpond--bounded-string (princ-to-string condition)))))

(defun completion-subscription--wake (subscription)
  "Invoke the optional wakeup outside locks; retain any failure for inspection."
  (when (completion-subscription--wakeup subscription)
    (handler-case (funcall (completion-subscription--wakeup subscription) subscription)
      (serious-condition (condition)
        (completion-subscription--record-error subscription condition))))
  nil)

(defun completion-subscription--send (subscription event)
  "Enqueue EVENT while holding the subscription lock; return duplicate status."
  (nth-value 1 (mailbox-send (completion-subscription-mailbox subscription)
                           :id (getf event :id) :sender "jobpond"
                           :receiver "completion" :payload event)))

(defun completion-subscription-refresh (subscription)
  "Reconcile terminal watches; return the count of newly queued events.
Callbacks run outside all locks and may repeat concurrently; they must be idempotent.
Failed watches retain job references after pool eviction for an explicit retry.
Enqueue order defines delivery order. Explicit calls propagate failures; listeners
retain the failure report and wake the caller."
  (let ((count 0)
        (watches (with-lock-held ((completion-subscription--lock subscription))
                   (unless (completion-subscription--closed-p subscription)
                     (copy-list (completion-subscription--watches subscription))))))
    (dolist (watch watches)
      (let* ((job (first watch)) (snapshot (job-snapshot job)))
        (when (member (getf snapshot :state) '(:completed :failed :aborted))
          (let* ((converted (funcall (completion-subscription--snapshot-function subscription)
                                    job snapshot))
                 (event (completion-event converted :id (rest watch))))
            (unless (and (equal (getf converted :identifier) (job-identifier job))
                         (eq (getf converted :state) (getf snapshot :state)))
              (durable-state--fail ':identity-conflict (rest watch)))
            (with-lock-held ((completion-subscription--lock subscription))
              (unless (completion-subscription--closed-p subscription)
                (unless (completion-subscription--send subscription event) (incf count))
                (setf (completion-subscription--watches subscription)
                      (remove watch (completion-subscription--watches subscription)))))))))
    (when (plusp count) (completion-subscription--wake subscription))
    count))

(defun completion-subscription--listen (subscription pool identity-function)
  "Install one listener under the subscription lock before snapshot reconciliation."
  (unless (assoc pool (completion-subscription--listeners subscription))
    (let ((listener
            (lambda (channel event)
              (when (and (eq channel :job-lifecycle)
                         (member (getf event :status) '(:completed :failed :aborted)))
                (handler-case
                    (let* ((job (getf event :job))
                           (id (when identity-function
                                 (funcall identity-function job (job-snapshot job)))))
                      (if id
                          (completion-subscription-watch subscription job :id id)
                          (completion-subscription-refresh subscription)))
                  (serious-condition (condition)
                    (completion-subscription--record-error subscription condition)
                    (completion-subscription--wake subscription)))))))
      (job-pool-add-listener pool listener)
      (push (cons pool listener) (completion-subscription--listeners subscription)))))

(defun completion-subscription-attach (subscription pool &key
                                       (identity-function
                                         (completion-subscription--identity-function subscription)))
  "Subscribe before admission and reconcile retained jobs without an attachment race.
IDENTITY-FUNCTION receives JOB and its coherent snapshot outside locks, returns an ID
or NIL to ignore it. This callback must be short and reliable: failed identity
conversion cannot retain an unidentified job. Use explicit WATCH to recover it.
Attach once per pool, before any explicit WATCH on that pool."
  (unless (functionp identity-function) (durable-state--fail ':configuration))
  (with-lock-held ((completion-subscription--lock subscription))
    (when (completion-subscription--closed-p subscription) (durable-state--fail ':closed))
    (when (assoc pool (completion-subscription--listeners subscription))
      (durable-state--fail ':already-attached))
    (completion-subscription--listen subscription pool identity-function))
  (dolist (job (job-pool-list-jobs pool))
    (let ((id (funcall identity-function job (job-snapshot job))))
      (when id (completion-subscription-watch subscription job :id id))))
  subscription)

(defun completion-subscription-watch (subscription job &key (id nil id-supplied-p))
  "Attach safely to running or terminal JOB using stable caller identity ID.
Install a listener before recording the watch and reconciling the snapshot.
Failed reconciliation retains the bounded watch for REFRESH, even after eviction.
Persist caller identities across process lifetimes; pool counters are local."
  (unless id-supplied-p
    (setf id (funcall (completion-subscription--identity-function subscription)
                      job (job-snapshot job))))
  (unless id (return-from completion-subscription-watch 0))
  (durable-state--identifier id)
  (with-lock-held ((completion-subscription--lock subscription))
    (when (completion-subscription--closed-p subscription) (durable-state--fail ':closed))
    (completion-subscription--listen subscription (job-pool job) nil)
    (let ((old (assoc job (completion-subscription--watches subscription))))
      (when (and old (not (equal id (rest old))))
        (durable-state--fail ':identity-conflict id))
      (unless old
        (when (>= (length (completion-subscription--watches subscription))
                  (completion-subscription--capacity subscription))
          (durable-state--fail ':capacity id))
        (setf (completion-subscription--watches subscription)
              (append (completion-subscription--watches subscription)
                      (list (cons job (copy-seq id))))))))
  (completion-subscription-refresh subscription))

(defun completion-subscription-replay (subscription snapshot &key id)
  "Reconstruct one persisted job SNAPSHOT; return EVENT and duplicate-p.
A nonterminal persisted job yields UNKNOWN because process restart cannot prove
its outcome. Supply terminal evidence instead when available. Conflicting evidence
for an already retained identity is refused rather than silently overwritten."
  (let ((event (completion-event snapshot :id id)) (duplicate nil))
    (with-lock-held ((completion-subscription--lock subscription))
      (when (completion-subscription--closed-p subscription) (durable-state--fail ':closed))
      (setf duplicate (completion-subscription--send subscription event)))
    (unless duplicate (completion-subscription--wake subscription))
    (values event duplicate)))

(defun completion-subscription-collect (subscription &key (limit 16))
  "Claim at most LIMIT queued delivery records in enqueue order as one batch.
Records contain ID, TOKEN and PAYLOAD (the complete event). Persist caller delivery
proof before ACK. Repeated collection never implicitly redelivers claimed records.
Reconcile current watches first so a failed producer callback can be retried."
  (unless (typep limit '(integer 1 *)) (durable-state--fail ':limit))
  (completion-subscription-refresh subscription)
  (loop repeat limit
        for record = (mailbox-receive (completion-subscription-mailbox subscription)
                                     :receiver "completion")
        while record collect record))

(defun completion-subscription-ack (subscription &key id token result)
  "Acknowledge ID and TOKEN after caller delivery proof is durable."
  (mailbox-ack (completion-subscription-mailbox subscription)
               :id id :receiver "completion" :token token :result result))

(defun completion-subscription-forget (subscription &key ids)
  "Forget exactly the settled IDS, returning their count; see MAILBOX-FORGET.
Retain caller delivery receipts and ignore receipted jobs in IDENTITY-FUNCTION
before pruning. Explicit replay/watch must likewise exclude forgotten identities.
Pending events and runtime watches are unaffected."
  (mailbox-forget (completion-subscription-mailbox subscription) :ids ids))

(defun completion-subscription-snapshot (subscription)
  "Return persisted event and delivery state, including retained deduplication IDs.
Runtime watches are not serialized. Persist the outstanding jobs separately and
replay their snapshots after restart; unknown execution outcomes stay explicit."
  (mailbox-snapshot (completion-subscription-mailbox subscription)))

(defun completion-subscription-close (subscription)
  "Detach listeners and release runtime job references without cancelling events.
Pending/claimed events remain inspectable and collectable. Closure prevents watch,
replay and refresh, but permits ACK and caller-owned delivery resolution. A callback
already in flight may finish its wakeup after closure. Return T on the first close."
  (with-lock-held ((completion-subscription--lock subscription))
    (unless (completion-subscription--closed-p subscription)
      (setf (completion-subscription--closed-p subscription) t)
      (dolist (entry (completion-subscription--listeners subscription))
        (job-pool-remove-listener (first entry) (rest entry)))
      (setf (completion-subscription--listeners subscription) nil
            (completion-subscription--watches subscription) nil)
      t)))
