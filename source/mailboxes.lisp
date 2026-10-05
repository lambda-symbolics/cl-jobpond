(in-package #:cl-jobpond)

;;;; -- Bounded Acknowledged Mailboxes --

(export '(mailbox make-mailbox mailbox-send mailbox-receive mailbox-ack mailbox-resolve
          mailbox-cancel mailbox-close mailbox-snapshot mailbox-wait mailbox-find))

(defclass mailbox (durable-state)
  ((waiters :initform nil :accessor mailbox--waiters
            :documentation "Private condition variables for all currently registered waiters."))
  (:documentation "Bounded queued and unacknowledged messages with durable identity history."))

(defmethod durable-state--changed ((mailbox mailbox))
  (dolist (condition (mailbox--waiters mailbox)) (condition-notify condition)))

(defun mailbox--validate (data)
  "Validate a portable version-one mailbox snapshot before recovering delivery claims."
  (handler-case
      (progn
        (durable-state--plist data '(:kind :format :sequence :capacity :history-limit :closed :messages))
        (unless (and (eq (getf data :kind) ':mailbox) (= (getf data :format) 1)
                     (typep (getf data :sequence) '(integer 0 *))
                     (typep (getf data :capacity) '(integer 1 *))
                     (typep (getf data :history-limit) '(integer 1 *))
                     (typep (getf data :closed) 'boolean) (listp (getf data :messages)))
          (durable-state--fail ':snapshot))
        (let ((identities (make-hash-table :test 'equal)))
          (dolist (message (getf data :messages))
          (durable-state--plist message '(:id :sender :receiver :payload :created-at :state :token :result :cancelled))
            (dolist (key '(:id :sender :receiver))
              (durable-state--identifier (getf message key)))
            (unless (and (member (getf message :state)
                                '(:queued :delivered :unknown :acknowledged :cancelled))
                         (typep (getf message :created-at) '(integer 0 *))
                         (typep (getf message :cancelled) 'boolean)
                       (or (not (eq (getf message :state) ':queued))
                           (and (not (getf message :cancelled)) (not (getf data :closed))))
                         (or (null (getf message :token))
                             (typep (getf message :token) `(integer 1 ,(getf data :sequence))))
                         (or (not (member (getf message :state) '(:delivered :unknown :acknowledged)))
                             (getf message :token))
                         (not (gethash (getf message :id) identities)))
              (durable-state--fail ':snapshot))
            (setf (gethash (getf message :id) identities) t)))
        (unless (and (<= (length (getf data :messages)) (getf data :history-limit))
                     (<= (mailbox--live-count data) (getf data :capacity)))
          (durable-state--fail ':capacity))
        data)
    (durable-state-error (condition) (error condition))
    (error () (durable-state--fail ':snapshot))))

(defun mailbox--live-count (data)
  "Count queued and all unacknowledged deliveries together."
  (count-if (lambda (message) (member (getf message :state) '(:queued :delivered :unknown)))
            (getf data :messages)))

(defun make-mailbox (&key (capacity 64) (history-limit 4096) (clock #'get-universal-time)
                         store snapshot)
  "Create a mailbox, recovering delivered messages as explicit UNKNOWN outcomes.
CAPACITY bounds queued plus unacknowledged messages. HISTORY-LIMIT bounds all
retained IDs; exhausted history refuses admission rather than dropping dedup
proof. STORE atomically replaces the supplied portable snapshot before visibility.
No callback may reenter the same mailbox. CLOCK returns integer time units."
  (unless (and (typep capacity '(integer 1 *)) (typep history-limit '(integer 1 *))
               (functionp clock) (or (null store) (functionp store)))
    (durable-state--fail ':configuration))
  (let* ((data (mailbox--validate
               (durable-state--copy
                (or snapshot (list :kind :mailbox :format 1 :sequence 0 :capacity capacity
                                   :history-limit history-limit :closed nil :messages nil)))))
         (mailbox (make-instance 'mailbox :data data :store store :clock clock)))
    (when snapshot
      (durable-state--transaction
       mailbox
       (lambda (data)
         (dolist (message (getf data :messages))
           (when (eq (getf message :state) ':delivered)
             (setf (getf message :state) ':unknown)))
         nil)))
    mailbox))

(defun mailbox-snapshot (mailbox)
  "Return a portable isolated snapshot including all retained message identities."
  (durable-state--snapshot mailbox))

(defun mailbox-find (mailbox id)
  "Return an isolated message record for ID, or signal NOT-FOUND."
  (durable-state--copy (durable-state--record (mailbox-snapshot mailbox) :messages id)))

(defun mailbox-send (mailbox &key id sender receiver payload)
  "Admit one message, or return its retained identical record and T for a duplicate.
Changed sender, receiver or payload for a retained ID signals IDENTITY-CONFLICT.
Admission refuses a full live mailbox or exhausted history. Duplicate lookup is
available even after closure, allowing a sender to resolve an uncertain send."
  (dolist (identity (list id sender receiver)) (durable-state--identifier identity))
  (let ((payload (durable-state--copy payload)) (now (durable-state--now mailbox)))
    (durable-state--transaction
     mailbox
     (lambda (data)
       (let ((old (find id (getf data :messages) :key (lambda (m) (getf m :id)) :test #'equal)))
         (when old
           (unless (and (equal sender (getf old :sender)) (equal receiver (getf old :receiver))
                        (equal payload (getf old :payload)))
             (durable-state--fail ':identity-conflict id))
           (return-from mailbox-send (values (durable-state--copy old) t))))
       (when (getf data :closed) (durable-state--fail ':closed id))
       (when (or (>= (mailbox--live-count data) (getf data :capacity))
                 (>= (length (getf data :messages)) (getf data :history-limit)))
         (durable-state--fail ':capacity id))
       (let ((message (list :id id :sender sender :receiver receiver :payload payload
                            :created-at now :state :queued :token nil :result nil :cancelled nil)))
         (setf (getf data :messages) (append (getf data :messages) (list message)))
         (values message nil))))))

(defun mailbox-receive (mailbox &key receiver id)
  "Claim the oldest queued message for RECEIVER, optionally restricted to ID.
The returned TOKEN belongs to this delivery. No capacity is released until ACK
or an explicit cancellation, and a claimed message is never redelivered implicitly."
  (durable-state--identifier receiver)
  (durable-state--transaction
   mailbox
   (lambda (data)
     (when (getf data :closed) (durable-state--fail ':closed))
     (let ((message (if id (durable-state--record data :messages id)
                        (find-if (lambda (m) (and (equal receiver (getf m :receiver))
                                                  (eq (getf m :state) ':queued)))
                                 (getf data :messages)))))
       (when message
         (unless (equal receiver (getf message :receiver))
           (durable-state--fail ':receiver id))
         (unless (eq (getf message :state) ':queued) (durable-state--fail ':not-queued id))
         (setf (getf message :state) ':delivered
               (getf message :token) (durable-state--token data))
         message)))))

(defun mailbox-ack (mailbox &key id receiver token result)
  "Acknowledge delivery using its receiver and current token, retaining optional RESULT.
Identical repeated acknowledgements are idempotent. Unknown recovered deliveries
may be acknowledged with their retained token; retry invalidates that token."
  (let ((result (durable-state--copy result)))
    (durable-state--transaction
     mailbox
     (lambda (data)
       (let ((message (durable-state--record data :messages id)))
         (unless (equal receiver (getf message :receiver)) (durable-state--fail ':receiver id))
         (unless (and token (eql token (getf message :token)))
           (durable-state--fail ':stale-token id))
         (cond ((member (getf message :state) '(:delivered :unknown))
                (setf (getf message :state) ':acknowledged (getf message :result) result))
               ((not (and (eq (getf message :state) ':acknowledged)
                          (equal result (getf message :result))))
                (durable-state--fail ':outcome-conflict id)))
         message)))))

(defun mailbox-resolve (mailbox &key id receiver token action)
  "Explicitly mark a delivery UNKNOWN or authorize RETRY after checking its outcome.
RETRY invalidates its token. It requires an open mailbox and no cancellation.
The receiver owns the decision whether repeating delivery is safe."
  (unless (member action '(:unknown :retry)) (durable-state--fail ':resolution id))
  (durable-state--transaction
   mailbox
   (lambda (data)
     (let ((message (durable-state--record data :messages id)))
       (unless (equal receiver (getf message :receiver)) (durable-state--fail ':receiver id))
       (unless (and token (eql token (getf message :token))
                    (member (getf message :state) '(:delivered :unknown)))
         (durable-state--fail ':stale-token id))
       (when (eq action ':retry)
         (when (getf data :closed) (durable-state--fail ':closed id))
         (when (getf message :cancelled) (durable-state--fail ':cancelled id)))
       (setf (getf message :state) (if (eq action ':retry) ':queued ':unknown))
       (when (eq action ':retry) (setf (getf message :token) nil))
       message))))

(defun mailbox-cancel (mailbox &key id sender)
  "Request cancellation by the original sender; only queued messages terminate immediately.
A delivered message retains its unacknowledged slot until its outcome is resolved."
  (durable-state--transaction
   mailbox
   (lambda (data)
     (let ((message (durable-state--record data :messages id)))
       (unless (equal sender (getf message :sender)) (durable-state--fail ':sender id))
       (setf (getf message :cancelled) t)
       (when (eq (getf message :state) ':queued) (setf (getf message :state) ':cancelled))
       message))))

(defun mailbox-close (mailbox)
  "Close admission/receive and cancel queued messages, retaining unacknowledged claims.
ACK and inspection remain available so in-flight outcomes can be settled."
  (durable-state--transaction
   mailbox
   (lambda (data)
     (setf (getf data :closed) t)
     (dolist (message (getf data :messages))
       (setf (getf message :cancelled) t)
       (when (eq (getf message :state) ':queued) (setf (getf message :state) ':cancelled)))
     t)))

(defun mailbox-wait (mailbox &key id receiver (timeout 2) cancelled-p)
  "Wait up to real TIMEOUT seconds for a terminal ID or queued message for RECEIVER.
ID waits for acknowledgement/cancellation; RECEIVER waits without claiming a queued
message. Return the isolated record and STATE, or NIL and TIMEOUT/CANCELLED/CLOSED.
Waits use condition notifications. An optional cancellation predicate is sampled
every 50ms outside the lock; TIMEOUT is independent of the injected durable clock."
  (unless (and (or (and id (null receiver)) (and receiver (null id)))
               (typep timeout '(real 0 *)) (or (null cancelled-p) (functionp cancelled-p)))
    (durable-state--fail ':wait))
  (when id (durable-state--identifier id))
  (when receiver (durable-state--identifier receiver))
  (let ((deadline (+ (get-internal-real-time) (* timeout internal-time-units-per-second)))
        (condition (make-condition-variable)))
    (with-lock-held ((durable-state-lock mailbox))
      (push condition (mailbox--waiters mailbox)))
    (unwind-protect
         (loop
           (when (and cancelled-p (funcall cancelled-p)) (return (values nil ':cancelled)))
           (with-lock-held ((durable-state-lock mailbox))
             (unless (durable-state--healthy-p mailbox) (durable-state--fail ':uncertain-store))
             (let* ((data (durable-state-data mailbox))
                    (message (if id (durable-state--record data :messages id)
                                 (find-if (lambda (m) (and (equal receiver (getf m :receiver))
                                                          (eq (getf m :state) ':queued)))
                                          (getf data :messages))))
                    (remaining (/ (- deadline (get-internal-real-time))
                                  internal-time-units-per-second)))
               (when (and message (or receiver (member (getf message :state) '(:acknowledged :cancelled))))
                 (return (values (durable-state--copy message) (getf message :state))))
               (when (getf data :closed) (return (values nil ':closed)))
               (when (<= remaining 0) (return (values nil ':timeout)))
               (condition-wait condition (durable-state-lock mailbox)
                               :timeout (if cancelled-p (min remaining 0.05) remaining)))))
      (with-lock-held ((durable-state-lock mailbox))
        (setf (mailbox--waiters mailbox) (remove condition (mailbox--waiters mailbox)))))))
