(in-package #:cl-jobpond)

;;;; -- Durable Wakeup State Machine --

(export '(scheduler make-scheduler scheduler-add scheduler-wake scheduler-event
          scheduler-claim scheduler-ack scheduler-resolve scheduler-cancel
          scheduler-snapshot scheduler-next-time scheduler-close))

(defclass scheduler (durable-state) ()
  (:documentation "Durable time, interval and event wakeups without an execution thread."))

(defun scheduler--occurrence-p (identity schedule)
  "Validate occurrence identity against its immutable schedule definition."
  (and (listp identity) (= (length identity) 3)
       (equal (first identity) (getf schedule :id))
       (equal (second identity) (getf schedule :version))
       (let ((occurrence (third identity)))
         (and (listp occurrence) (= (length occurrence) 2)
              (if (eq (getf schedule :mode) ':event)
                  (and (eq (first occurrence) ':event)
                       (stringp (second occurrence)) (<= 1 (length (second occurrence)) 256))
                  (and (eq (first occurrence) ':time)
                       (typep (second occurrence) '(integer 0 *))
                       (if (getf schedule :interval)
                           (and (>= (second occurrence) (getf schedule :at))
                                (zerop (mod (- (second occurrence) (getf schedule :at))
                                            (getf schedule :interval))))
                           (= (second occurrence) (getf schedule :at)))))))))

(defun scheduler--validate (data)
  "Validate a portable version-one scheduler snapshot before recovery."
  (handler-case
      (progn
        (durable-state--plist data '(:kind :format :sequence :capacity :closed :schedules :wakeups))
        (unless (and (eq (getf data :kind) ':scheduler) (= (getf data :format) 1)
                     (typep (getf data :sequence) '(integer 0 *))
                     (typep (getf data :capacity) '(integer 1 *))
                     (typep (getf data :closed) 'boolean)
                     (listp (getf data :schedules)) (listp (getf data :wakeups)))
          (durable-state--fail ':snapshot))
        (let ((identities (make-hash-table :test 'equal)))
          (dolist (schedule (getf data :schedules))
          (durable-state--plist schedule
                                '(:id :version :payload :at :interval :event :missed-policy
                                  :specification :mode :next :cancelled))
          (durable-state--plist (getf schedule :specification)
                                '(:id :version :payload :at :interval :event :missed-policy))
          (dolist (key '(:id :version :payload :at :interval :event :missed-policy))
            (unless (equal (getf schedule key) (getf (getf schedule :specification) key))
              (durable-state--fail ':snapshot)))
            (durable-state--identifier (getf schedule :id))
            (durable-state--identifier (getf schedule :version))
            (when (gethash (getf schedule :id) identities)
              (durable-state--fail ':duplicate-identity))
            (setf (gethash (getf schedule :id) identities) t)
            (unless (and (member (getf schedule :mode) '(:time :interval :event))
                         (member (getf schedule :missed-policy) '(:skip :latest :all))
                         (typep (getf schedule :cancelled) 'boolean)
                       (case (getf schedule :mode)
                         (:event
                          (and (null (getf schedule :at)) (null (getf schedule :next))
                               (null (getf schedule :interval))
                               (progn (durable-state--identifier (getf schedule :event)) t)))
                         (:time
                          (and (typep (getf schedule :at) '(integer 0 *))
                               (null (getf schedule :interval)) (null (getf schedule :event))
                               (or (null (getf schedule :next))
                                   (eql (getf schedule :next) (getf schedule :at)))))
                         (:interval
                          (and (typep (getf schedule :at) '(integer 0 *))
                               (typep (getf schedule :next) '(integer 0 *))
                               (typep (getf schedule :interval) '(integer 1 *))
                               (null (getf schedule :event))
                               (>= (getf schedule :next) (getf schedule :at))
                               (zerop (mod (- (getf schedule :next) (getf schedule :at))
                                           (getf schedule :interval)))))))
              (durable-state--fail ':snapshot)))
          (clrhash identities)
          (dolist (wakeup (getf data :wakeups))
          (durable-state--plist wakeup '(:id :schedule :version :payload :event-data :created-at
                                        :state :token :result :cancelled))
            (let ((schedule (durable-state--record data :schedules (getf wakeup :schedule))))
              (unless (and (equal (getf schedule :version) (getf wakeup :version))
                         (equal (getf wakeup :payload) (getf schedule :payload))
                         (scheduler--occurrence-p (getf wakeup :id) schedule)
                         (or (not (eq (getf wakeup :state) ':pending))
                             (and (not (getf wakeup :cancelled)) (not (getf schedule :cancelled))
                                  (not (getf data :closed))))
                           (member (getf wakeup :state)
                                   '(:pending :claimed :unknown :completed :failed :cancelled))
                           (typep (getf wakeup :cancelled) 'boolean)
                           (typep (getf wakeup :created-at) '(integer 0 *))
                           (or (null (getf wakeup :token))
                               (typep (getf wakeup :token)
                                      `(integer 1 ,(getf data :sequence))))
                         (or (not (member (getf wakeup :state) '(:claimed :unknown :completed :failed)))
                               (getf wakeup :token))
                           (not (gethash (getf wakeup :id) identities)))
                (durable-state--fail ':snapshot))
              (setf (gethash (getf wakeup :id) identities) t))))
        (unless (and (<= (length (getf data :wakeups)) (getf data :capacity))
                     (<= (length (getf data :schedules)) (getf data :capacity)))
          (durable-state--fail ':capacity))
        data)
    (durable-state-error (condition) (error condition))
    (error () (durable-state--fail ':snapshot))))

(defun make-scheduler (&key (clock #'get-universal-time) store snapshot (capacity 4096))
  "Create a scheduler, recovering claimed wakeups as UNKNOWN.
STORE accepts one complete snapshot and must atomically durably replace it or
signal without changing storage. CLOCK returns nonnegative integer time units.
CAPACITY bounds each retained schedule/wakeup collection. No automatic eviction
weakens deduplication; choose a new store namespace when retiring the history."
  (unless (and (functionp clock) (or (null store) (functionp store))
               (typep capacity '(integer 1 *)))
    (durable-state--fail ':configuration))
  (let* ((data (scheduler--validate
               (durable-state--copy
                (or snapshot (list :kind :scheduler :format 1 :sequence 0
                                   :capacity capacity :closed nil :schedules nil :wakeups nil)))))
         (scheduler (make-instance 'scheduler :data data :clock clock :store store)))
    (when snapshot
      (durable-state--transaction
       scheduler
       (lambda (data)
         (dolist (wakeup (getf data :wakeups))
           (when (eq (getf wakeup :state) ':claimed)
             (setf (getf wakeup :state) ':unknown)))
         nil)))
    scheduler))

(defun scheduler-snapshot (scheduler)
  "Return a portable isolated scheduler snapshot including retained dedup history."
  (durable-state--snapshot scheduler))

(defun scheduler--open (data)
  "Refuse mutations that enqueue new work after closure."
  (when (getf data :closed) (durable-state--fail ':closed)))

(defun scheduler-add (scheduler &key id version payload at interval event (missed-policy ':latest))
  "Register a stable immutable schedule. Duplicate identical registrations are idempotent.
AT supplies the initial time for time/interval schedules. EVENT is an external
event name and excludes AT/INTERVAL. MISSED-POLICY is SKIP, LATEST or ALL. Use a
new ID for a replacement version; cancelled IDs cannot be silently resurrected."
  (durable-state--identifier id)
  (durable-state--identifier version)
  (unless (and (member missed-policy '(:skip :latest :all))
               (if event
                   (and (stringp event) (plusp (length event)) (null at) (null interval))
                   (and (typep at '(integer 0 *))
                        (or (null interval) (typep interval '(integer 1 *))))))
    (durable-state--fail ':schedule id))
  (when event (durable-state--identifier event))
  (let* ((specification (list :id id :version version :payload payload :at at
                             :interval interval :event event :missed-policy missed-policy))
         (record (append (durable-state--copy specification)
                         (list :specification (durable-state--copy specification)
                               :mode (cond (event ':event) (interval ':interval) (t ':time))
                               :next at :cancelled nil))))
    (durable-state--transaction
     scheduler
     (lambda (data)
       (scheduler--open data)
       (let ((old (find id (getf data :schedules) :key (lambda (s) (getf s :id))
                        :test #'equal)))
         (when old
           (unless (equal specification (getf old :specification))
             (durable-state--fail ':identity-conflict id))
           (return-from scheduler-add (durable-state--copy old))))
       (when (>= (length (getf data :schedules)) (getf data :capacity))
         (durable-state--fail ':capacity id))
       (setf (getf data :schedules) (append (getf data :schedules) (list record)))
       record))))

(defun scheduler--enqueue (data schedule discriminator now &optional event-data)
  "Enqueue one occurrence unless its stable ID was previously retained."
  (let ((identity (list (getf schedule :id) (getf schedule :version) discriminator)))
    (unless (find identity (getf data :wakeups) :key (lambda (w) (getf w :id)) :test #'equal)
      (when (>= (length (getf data :wakeups)) (getf data :capacity))
        (durable-state--fail ':capacity identity))
      (let ((wakeup (list :id identity :schedule (getf schedule :id)
                          :version (getf schedule :version) :payload (getf schedule :payload)
                          :event-data event-data :created-at now :state :pending
                          :token nil :result nil :cancelled nil)))
        (setf (getf data :wakeups) (append (getf data :wakeups) (list wakeup)))
        wakeup))))

(defun scheduler-wake (scheduler &key now)
  "Materialize due time wakeups atomically, returning newly enqueued records.
LATEST coalesces overdue intervals to their most recent occurrence. SKIP omits
past occurrences but emits an occurrence exactly at NOW. ALL catches up every
due occurrence, refusing the entire transaction if retention capacity is exceeded."
  (let ((now (or now (durable-state--now scheduler))))
    (unless (typep now '(integer 0 *)) (durable-state--fail ':clock))
    (durable-state--transaction
     scheduler
     (lambda (data)
       (scheduler--open data)
       (let ((new nil))
         (dolist (schedule (getf data :schedules))
           (let ((next (getf schedule :next)) (interval (getf schedule :interval)))
             (when (and (not (getf schedule :cancelled)) next (<= next now))
               (let* ((latest (if interval (+ next (* interval (floor (- now next) interval))) next))
                      (times (case (getf schedule :missed-policy)
                               (:all (loop for time from next to latest by (or interval 1)
                                           for count from 1
                                           do (when (> count (getf data :capacity))
                                                (durable-state--fail ':capacity))
                                           collect time))
                               (:latest (list latest))
                               (:skip (when (= latest now) (list latest))))))
                 (dolist (time times)
                   (let ((wakeup (scheduler--enqueue data schedule (list :time time) now)))
                     (when wakeup (push wakeup new))))
                 (setf (getf schedule :next) (and interval (+ latest interval)))))))
         (nreverse new))))))

(defun scheduler-event (scheduler &key event id payload)
  "Deliver one stable external event ID, enqueueing each matching active schedule once."
  (durable-state--identifier event)
  (durable-state--identifier id)
  (let ((now (durable-state--now scheduler)) (payload (durable-state--copy payload)))
    (durable-state--transaction
     scheduler
     (lambda (data)
       (scheduler--open data)
       (let ((new nil))
         (dolist (schedule (getf data :schedules))
           (when (and (eq (getf schedule :mode) ':event)
                      (equal event (getf schedule :event)) (not (getf schedule :cancelled)))
             (let* ((identity (list (getf schedule :id) (getf schedule :version) (list :event id)))
                    (old (find identity (getf data :wakeups) :key (lambda (w) (getf w :id)) :test #'equal)))
               (when (and old (not (equal payload (getf old :event-data))))
                 (durable-state--fail ':identity-conflict identity))
               (let ((wakeup (scheduler--enqueue data schedule (list :event id) now payload)))
                 (when wakeup (push wakeup new))))))
         (nreverse new))))))

(defun scheduler-claim (scheduler &key id version)
  "Atomically claim a pending wakeup, returning its record with a new persisted TOKEN.
ID and VERSION optionally restrict the claim. A stale expected version is refused.
The caller owns admission into its existing execution facility and must ACK or
explicitly RESOLVE an uncertain outcome; no automatic reexecution occurs."
  (durable-state--transaction
   scheduler
   (lambda (data)
     (scheduler--open data)
     (let ((wakeup (if id (durable-state--record data :wakeups id)
                       (find ':pending (getf data :wakeups) :key (lambda (w) (getf w :state))))))
       (when wakeup
         (when (and version (not (equal version (getf wakeup :version))))
           (durable-state--fail ':stale-version id))
         (unless (eq (getf wakeup :state) ':pending)
           (durable-state--fail ':not-pending (getf wakeup :id)))
         (setf (getf wakeup :state) ':claimed
               (getf wakeup :token) (durable-state--token data))
         wakeup)))))

(defun scheduler-ack (scheduler &key id token (outcome ':completed) result)
  "Acknowledge a claimed/unknown outcome using its current TOKEN.
Repeated identical acknowledgements are idempotent. RESULT is retained portable
proof supplied by the host. A stale token or conflicting outcome is refused."
  (unless (member outcome '(:completed :failed :cancelled))
    (durable-state--fail ':outcome id))
  (let ((result (durable-state--copy result)))
    (durable-state--transaction
     scheduler
     (lambda (data)
       (let ((wakeup (durable-state--record data :wakeups id)))
         (unless (and token (eql token (getf wakeup :token)))
           (durable-state--fail ':stale-token id))
         (cond ((member (getf wakeup :state) '(:claimed :unknown))
                (setf (getf wakeup :state) outcome (getf wakeup :result) result))
               ((not (and (eq (getf wakeup :state) outcome)
                          (equal (getf wakeup :result) result)))
                (durable-state--fail ':outcome-conflict id)))
         wakeup)))))

(defun scheduler-resolve (scheduler &key id token action)
  "Explicitly declare a claim UNKNOWN or authorize RETRY after an uncertain outcome.
RETRY invalidates the old token and is refused after cancellation/closure. The
caller must decide whether repeating externally visible effects is safe."
  (unless (member action '(:unknown :retry)) (durable-state--fail ':resolution id))
  (durable-state--transaction
   scheduler
   (lambda (data)
     (let ((wakeup (durable-state--record data :wakeups id)))
       (unless (and token (eql token (getf wakeup :token))
                    (member (getf wakeup :state) '(:claimed :unknown)))
         (durable-state--fail ':stale-token id))
       (when (eq action ':retry)
         (scheduler--open data)
         (when (getf wakeup :cancelled) (durable-state--fail ':cancelled id)))
       (setf (getf wakeup :state) (if (eq action ':retry) ':pending ':unknown))
       (when (eq action ':retry) (setf (getf wakeup :token) nil))
       wakeup))))

(defun scheduler-cancel (scheduler &key id version)
  "Cancel future/pending work for ID, preserving claimed outcomes for explicit ACK."
  (durable-state--transaction
   scheduler
   (lambda (data)
     (let ((schedule (durable-state--record data :schedules id)))
       (when (and version (not (equal version (getf schedule :version))))
         (durable-state--fail ':stale-version id))
       (setf (getf schedule :cancelled) t)
       (dolist (wakeup (getf data :wakeups))
         (when (equal id (getf wakeup :schedule))
           (setf (getf wakeup :cancelled) t)
           (when (eq (getf wakeup :state) ':pending)
             (setf (getf wakeup :state) ':cancelled))))
       schedule))))

(defun scheduler-next-time (scheduler)
  "Return the earliest active time wakeup, or NIL when only events/no work remain."
  (let ((times (loop for schedule in (getf (scheduler-snapshot scheduler) :schedules)
                     when (and (not (getf schedule :cancelled)) (getf schedule :next))
                       collect (getf schedule :next))))
    (when times (reduce #'min times))))

(defun scheduler-close (scheduler)
  "Idempotently close admission and cancel pending work; claimed outcomes remain explicit."
  (durable-state--transaction
   scheduler
   (lambda (data)
     (setf (getf data :closed) t)
     (dolist (schedule (getf data :schedules)) (setf (getf schedule :cancelled) t))
     (dolist (wakeup (getf data :wakeups))
       (setf (getf wakeup :cancelled) t)
       (when (eq (getf wakeup :state) ':pending) (setf (getf wakeup :state) ':cancelled)))
     t)))
