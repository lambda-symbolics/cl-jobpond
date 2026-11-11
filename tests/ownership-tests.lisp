(in-package #:cl-jobpond/tests)

;;;; -- Cross-pool Ownership --

(defun tests--retention-successor (pool)
  "Publish a successor to evict the previous terminal job from a one-entry pool."
  (job-run-inline (job-pool-submit pool (constantly nil) :inline-only-p t)))

(defun tests--terminal-bridge ()
  "Keep a live cross-pool leaf visible through evicted terminal ancestors."
  (let ((started (tests--make-gate))
        (release (tests--make-gate))
        (settled (tests--make-gate)))
    (with-test-pool (roots :name "ownership roots" :terminal-retention-limit 1)
      (with-test-pool (bridges :name "ownership bridges" :terminal-retention-limit 1)
        (with-test-pool (leaves :name "ownership leaves" :terminal-retention-limit 1
                                :job-class 'tests--cooperative-job)
          (let* ((root (job-pool-submit roots (constantly :root) :inline-only-p t))
                 (bridge (job-pool-submit bridges (constantly :bridge)
                                          :parent-job root :inline-only-p t))
                 (leaf (job-pool-submit
                        leaves
                        (lambda (job)
                          (declare (ignore job))
                          (tests--gate-open started)
                          (tests--gate-await release)
                          :leaf)
                        :parent-job bridge)))
            (unwind-protect
                 (progn
                   (test-assert (tests--gate-await started) "the cross-pool leaf starts")
                   (job-pool-add-listener
                    leaves
                    (lambda (channel event)
                      (when (and (eq channel :job-lifecycle)
                                 (eq (getf event :job) leaf)
                                 (eq (getf event :status) :aborted))
                        (tests--gate-open settled))))
                   (job-run-inline bridge)
                   (job-run-inline root)
                   (tests--retention-successor roots)
                   (tests--retention-successor bridges)
                   (test-assert (not (member root (job-pool-list-jobs roots))) "the terminal root is evicted")
                   (test-assert (not (member bridge (job-pool-list-jobs bridges))) "the terminal bridge is evicted")
                   (multiple-value-bind (accepted descendants)
                       (job-cancel root :cascade-p t)
                     (test-assert (not accepted) "a terminal root does not accept a new cancellation")
                     (test-assert (equal descendants (list leaf))
                                  "cancellation traverses the terminal cross-pool bridge"))
                   (test-assert (job-cancellation-requested-p leaf) "the live leaf is marked")
                   (test-assert (not (nth-value 1 (job-await leaf :timeout-seconds 0)))
                                "cancellation does not imply descendant settlement")
                   (multiple-value-bind (accepted descendants)
                       (job-cancel root :cascade-p t)
                     (test-assert (not accepted) "repeated root cancellation is idempotent")
                     (test-assert (equal descendants (list leaf))
                                  "repeated cancellation returns unfinished ownership"))
                   (bordeaux-threads:with-recursive-lock-held (cl-jobpond::*job-tree-lock*)
                     (test-assert (member bridge (cl-jobpond::job--child-jobs root))
                                  "the terminal bridge is retained while its leaf lives"))
                   (tests--gate-open release)
                   (test-assert (tests--gate-await settled) "the leaf publishes after explicit release")
                   (tests--retention-successor leaves)
                   (test-assert (not (member leaf (job-pool-list-jobs leaves))) "the terminal leaf is evicted")
                   (bordeaux-threads:with-recursive-lock-held (cl-jobpond::*job-tree-lock*)
                     (test-assert (null (cl-jobpond::job--child-jobs root))
                                  "a settled branch releases the root's strong child link")
                     (test-assert (null (cl-jobpond::job--child-jobs bridge))
                                  "a settled branch releases the bridge's strong child link")
                     (test-assert (and (null (cl-jobpond:job-parent-job leaf))
                                       (null (cl-jobpond:job-parent-job bridge)))
                                  "settled children release ancestor references")))
              (tests--gate-open release))))))))

(defun tests--prompt-cascade ()
  "Separate prompt cancellation from blocking queued hooks and running cleanup."
  (let ((started (tests--make-gate))
        (interrupted (tests--make-gate))
        (cleanup-release (tests--make-gate))
        (hook-entered (tests--make-gate))
        (hook-release (tests--make-gate))
        (cancel-returned (tests--make-gate))
        (selected nil)
        (caller nil))
    (with-test-pool (roots :name "prompt cancellation roots")
      (with-test-pool (leaves :name "prompt cancellation leaves")
        (let* ((root (job-pool-submit
                      roots (constantly :unused) :inline-only-p t
                      :terminal-result-function
                      (lambda (job state result report)
                        (declare (ignore job))
                        (tests--gate-open hook-entered)
                        (tests--gate-await hook-release)
                        (values result report state))))
               (leaf (job-pool-submit
                      leaves
                      (lambda (job)
                        (declare (ignore job))
                        (unwind-protect
                             (progn
                               (tests--gate-open started)
                               (tests--gate-await hook-release))
                          (tests--gate-open interrupted)
                          (tests--gate-await cleanup-release)))
                      :parent-job root)))
          (unwind-protect
               (progn
                 (test-assert (tests--gate-await started) "the interruptible leaf starts")
                 (setf caller
                       (make-thread
                        (lambda ()
                          (setf selected (multiple-value-list (job-cancel root :cascade-p t)))
                          (tests--gate-open cancel-returned))
                        :name "prompt cancellation caller"))
                 (test-assert (tests--gate-await cancel-returned :timeout-seconds 2)
                              "cancellation returns while queued hooks and cleanup are blocked")
                 (test-assert (first selected) "queued root cancellation is accepted")
                 (test-assert (equal (second selected) (list leaf)) "the selected live leaf is returned")
                 (test-assert (tests--gate-await hook-entered :timeout-seconds 2)
                              "the queued terminal hook runs asynchronously")
                 (test-assert (tests--gate-await interrupted :timeout-seconds 2)
                              "the leaf is interrupted before the queued hook can finish")
                 (test-assert (and (job-cancellation-requested-p root)
                                   (job-cancellation-requested-p leaf))
                              "all selected jobs are marked before blocked publication")
                 (test-assert (not (nth-value 1 (job-await root :timeout-seconds 0))) "the blocked root is not terminal")
                 (test-assert (not (nth-value 1 (job-await leaf :timeout-seconds 0))) "the blocked leaf cleanup is not terminal")
                 (let ((cl-jobpond:*shutdown-timeout-seconds* 0.03))
                   (test-assert (not (job-pool-close roots))
                                "bounded shutdown retains the blocked cancellation delivery owner"))
                 (test-assert (handler-case
                                  (progn (cl-jobpond:job-pool-refresh roots) nil)
                                (cl-jobpond:job-pool-closed () t))
                              "a pool cannot refresh while its cancellation hook is unfinished")
                 (tests--gate-open hook-release)
                 (test-assert (eq (getf (tests--await-completed root) :state) :aborted)
                              "the root may publish before its descendant settles")
                 (test-assert (equal (second (multiple-value-list (job-cancel root :cascade-p t)))
                                     (list leaf))
                              "terminal publication preserves unfinished descendant ownership")
                 (tests--gate-open cleanup-release)
                 (test-assert (eq (getf (tests--await-completed leaf) :state) :aborted)
                              "the separately awaited descendant finishes aborted"))
            (tests--gate-open hook-release)
            (tests--gate-open cleanup-release)
            (when caller (join-thread caller))))))))

;;;; -- Admission Races --

(defclass tests--admission-gated-job (cl-jobpond:job)
  ((entered :initarg :entered :initform nil :reader tests--admission-entered
            :documentation "Gate opened during creation inside admission.")
   (release :initarg :release :initform nil :reader tests--admission-release
            :documentation "Gate releasing the admission commit."))
  (:documentation "A fixture that suspends creation before queue and link publication."))

(defmethod initialize-instance :after ((job tests--admission-gated-job) &key)
  "Expose a deterministic admission window while the pool lock is held."
  (when (tests--admission-entered job)
    (tests--gate-open (tests--admission-entered job))
    (tests--gate-await (tests--admission-release job))))

(defun tests--admission-cancellation-race ()
  "A concurrent cascade includes an admission winner and rejects a later loser."
  (let ((entered (tests--make-gate))
        (release (tests--make-gate))
        (admitted (tests--make-gate))
        (cancel-started (tests--make-gate))
        (cancel-returned (tests--make-gate))
        (child nil) (selection nil) (submitter nil) (canceller nil))
    (with-test-pool (roots :name "admission race roots")
      (with-test-pool (children :name "admission race children"
                                :job-class 'tests--admission-gated-job)
        (let ((root (job-pool-submit roots (constantly nil) :inline-only-p t)))
          (unwind-protect
               (progn
                 (setf submitter
                       (make-thread
                        (lambda ()
                          (setf child
                                (first (job-pool-submit-batch
                                        children
                                        (list (list :function (constantly :unused)
                                                    :parent-job root :inline-only-p t
                                                    :initargs (list :entered entered :release release))))))
                          (tests--gate-open admitted))
                        :name "ownership admission contender"))
                 (test-assert (tests--gate-await entered) "admission pauses before visibility")
                 (setf canceller
                       (make-thread
                        (lambda ()
                          (tests--gate-open cancel-started)
                          (setf selection (multiple-value-list (job-cancel root :cascade-p t)))
                          (tests--gate-open cancel-returned))
                        :name "ownership cancellation contender"))
                 (test-assert (tests--gate-await cancel-started) "the cancellation contender starts")
                 (tests--gate-open release)
                 (test-assert (tests--gate-await admitted :timeout-seconds 2) "the admission winner commits")
                 (test-assert (tests--gate-await cancel-returned :timeout-seconds 2)
                              "the cascade completes after the atomic admission")
                 (test-assert (and (first selection) (member child (second selection)))
                              "a child admitted during selection cannot escape cancellation")
                 (test-assert (job-cancellation-requested-p child) "the admitted child is marked")
                 (test-assert (eq (job-pool-find-job children (job-identifier child)) child)
                              "admission inserts the job into the pool table")
                 (test-assert (handler-case
                                  (progn (job-pool-submit children (constantly :escaped)
                                                          :parent-job root :inline-only-p t)
                                         nil)
                                (cl-jobpond:job-aborted () t))
                              "a cancellation winner rejects subsequent descendant admission")
                 (test-assert (= (length (job-pool-list-jobs children)) 1)
                              "rejected admission creates no pool record")
                 (tests--await-completed child)
                 (tests--await-completed root))
            (tests--gate-open release)
            (when submitter (join-thread submitter))
            (when canceller (join-thread canceller))))))))

(defun tests--nested-admission ()
  "Admit a linked child inside the parent's admission transaction without inversion."
  (let ((started (tests--make-gate))
        (release (tests--make-gate))
        (admitted (tests--make-gate))
        (runner nil) (child nil))
    (with-test-pool (parents :name "nested admission parents"
                             :job-class 'tests--cooperative-job)
      (with-test-pool (children :name "nested admission children")
        (let ((parent (job-pool-submit
                       parents
                       (lambda (job)
                         (declare (ignore job))
                         (tests--gate-open started)
                         (tests--gate-await release)))))
          (unwind-protect
               (progn
                 (test-assert (tests--gate-await started) "the parent is running")
                 (setf runner
                       (make-thread
                        (lambda ()
                          (cl-jobpond:job-call-with-admission
                           parent
                           (lambda ()
                             (setf child (job-pool-submit children (constantly :child)
                                                          :parent-job parent :inline-only-p t))))
                          (tests--gate-open admitted))
                        :name "nested ownership admission"))
                 (test-assert (tests--gate-await admitted :timeout-seconds 2)
                              "nested cross-pool admission does not reenter the parent lifecycle lock")
                 (test-assert (eq (job-pool-find-job children (job-identifier child)) child)
                              "the nested admission publishes a findable child")
                 (job-run-inline child)
                 (tests--gate-open release)
                 (test-assert (eq (getf (tests--await-completed parent) :state) :completed)
                              "the parent publishes after nested admission"))
            (tests--gate-open release)
            (when runner (join-thread runner))))))))

(defvar *tests--parent-lock-requested* nil
  "A gate dynamically bound only by the admission lock-order contender.")

(defclass tests--lock-probe-job (cl-jobpond:job) ()
  (:documentation "Expose a parent lifecycle-lock request to the lock-order test."))

(defmethod cl-jobpond::job--lock :around ((job tests--lock-probe-job))
  "Signal the contender's exact parent-lock acquisition boundary."
  (when *tests--parent-lock-requested*
    (tests--gate-open *tests--parent-lock-requested*))
  (call-next-method))

(defun tests--admission-lock-order ()
  "Acquire a pool lock while a real admission waits for its parent's job lock."
  (let ((requested (tests--make-gate))
        (committed (tests--make-gate))
        (submitter nil) (child nil))
    (with-test-pool (parents :name "lock-order parents" :job-class 'tests--lock-probe-job)
      (with-test-pool (children :name "lock-order children")
        (let ((parent (job-pool-submit parents (constantly nil) :inline-only-p t)))
          (unwind-protect
               (progn
                 (with-lock-held ((cl-jobpond::job--lock parent))
                   (setf submitter
                         (make-thread
                          (lambda ()
                            (let ((*tests--parent-lock-requested* requested))
                              (setf child (job-pool-submit children (constantly nil)
                                                           :parent-job parent :inline-only-p t)))
                            (tests--gate-open committed))
                          :name "parent lock-order contender"))
                   (test-assert (tests--gate-await requested :timeout-seconds 2)
                                "admission reaches its parent lifecycle lock")
                   (let* ((lock (cl-jobpond::job-pool--lock children))
                          (acquired (bordeaux-threads:acquire-lock lock nil)))
                     (when acquired (bordeaux-threads:release-lock lock))
                     (test-assert acquired
                                  "admission never holds the pool lock while waiting for a parent")))
                 (test-assert (tests--gate-await committed :timeout-seconds 2)
                              "admission commits once the parent lock is released")
                 (test-assert (eq (job-pool-find-job children (job-identifier child)) child)
                              "the lock-order winner retains its pool record")
                 (job-run-inline child)
                 (job-run-inline parent))
            (when submitter (join-thread submitter))))))))

(defun tests--failed-interrupt-delivery ()
  "Continue sibling notification and queued publication after one interrupt fails."
  (let ((started-left (tests--make-gate))
        (started-right (tests--make-gate))
        (release (tests--make-gate))
        (original (symbol-function 'bordeaux-threads:interrupt-thread))
        (attempts 0))
    (with-test-pool (roots :name "failed delivery roots")
      (with-test-pool (left-pool :name "failed delivery left")
        (with-test-pool (right-pool :name "failed delivery right")
          (let* ((root (job-pool-submit roots (constantly nil) :inline-only-p t))
                 (left (job-pool-submit
                        left-pool
                        (lambda (job)
                          (declare (ignore job))
                          (tests--gate-open started-left)
                          (tests--gate-await release))
                        :parent-job root))
                 (right (job-pool-submit
                         right-pool
                         (lambda (job)
                           (declare (ignore job))
                           (tests--gate-open started-right)
                           (tests--gate-await release))
                         :parent-job root)))
            (unwind-protect
                 (progn
                   (test-assert (tests--gate-await started-left) "the first sibling is running")
                   (test-assert (tests--gate-await started-right) "the second sibling is running")
                   (setf (symbol-function 'bordeaux-threads:interrupt-thread)
                         (lambda (thread function)
                           (incf attempts)
                           (if (= 1 attempts)
                               (error "Synthetic dead-worker interrupt race.")
                               (funcall original thread function))))
                   (multiple-value-bind (accepted descendants) (job-cancel root :cascade-p t)
                     (test-assert accepted "the queued parent accepts cancellation")
                     (test-assert (and (member left descendants) (member right descendants))
                                  "both running siblings retain cancellation ownership"))
                   (test-assert (nth-value 1 (job-await root :timeout-seconds 2))
                                "one failed interrupt cannot strand queued publication")
                   (test-assert (eq ':aborted (job-state root)) "the queued parent publishes its abort")
                   (test-assert (= 2 attempts) "later sibling delivery is attempted after failure")
                   (test-assert (and (job-cancellation-requested-p left)
                                     (job-cancellation-requested-p right))
                                "all selected siblings keep their cancellation marks"))
              (setf (symbol-function 'bordeaux-threads:interrupt-thread) original)
              (tests--gate-open release)))))))
  nil)

(defun tests--ownership-protocol ()
  "Run synchronized cross-pool cancellation and admission ownership cases."
  (tests--terminal-bridge)
  (tests--prompt-cascade)
  (tests--failed-interrupt-delivery)
  (tests--admission-cancellation-race)
  (tests--nested-admission)
  (tests--admission-lock-order)
  nil)
