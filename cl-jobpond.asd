(asdf:defsystem #:cl-jobpond
  :description "Supervised bounded thread pool for cancellable in-process jobs"
  :author "Lukáš Hozda"
  :license "COLL-Attribution"
  :version "0.1.0"
  :serial t
  :depends-on (#:bordeaux-threads)
  :components ((:module "source"
                :serial t
                :components ((:file "package")
                             (:file "conditions")
                             (:file "pool")
                             (:file "inspection")
                             (:file "publication")
                             (:file "cancellation")
                             (:file "lifecycle")
                             (:file "admission"))))
  :in-order-to ((asdf:test-op (asdf:test-op #:cl-jobpond/tests))))

(asdf:defsystem #:cl-jobpond/tests
  :description "Tests for cl-jobpond"
  :depends-on (#:cl-jobpond)
  :serial t
  :components ((:module "tests"
                :serial t
                :components ((:file "package")
                             (:file "tests")
                             (:file "cancellation-policy-tests"))))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call '#:cl-jobpond/tests '#:run-tests)))


(asdf:defsystem #:cl-jobpond/durable-state
  :description "Portable transactional state shared by optional schedules and mailboxes"
  :depends-on (#:cl-jobpond)
  :components ((:file "source/durable-state")))

(asdf:defsystem #:cl-jobpond/schedules
  :description "Durable time, interval and event wakeup state machine"
  :depends-on (#:cl-jobpond/durable-state)
  :components ((:file "source/schedules"))
  :in-order-to ((asdf:test-op (asdf:test-op #:cl-jobpond/coordination-tests))))

(asdf:defsystem #:cl-jobpond/mailboxes
  :description "Bounded acknowledged mailboxes with stable delivery identities"
  :depends-on (#:cl-jobpond/durable-state)
  :components ((:file "source/mailboxes"))
  :in-order-to ((asdf:test-op (asdf:test-op #:cl-jobpond/coordination-tests))))

(asdf:defsystem #:cl-jobpond/coordination-tests
  :description "Persistence, concurrency and failure tests for schedules and mailboxes"
  :depends-on (#:cl-jobpond/schedules #:cl-jobpond/mailboxes)
  :components ((:file "tests/coordination-tests"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call '#:cl-jobpond/tests '#:run-coordination-tests)))


(asdf:defsystem #:cl-jobpond/completions
  :description "Durable bounded terminal subscriptions using acknowledged mailboxes"
  :depends-on (#:cl-jobpond/mailboxes)
  :components ((:file "source/completions"))
  :in-order-to ((asdf:test-op (asdf:test-op #:cl-jobpond/completion-tests))))

(asdf:defsystem #:cl-jobpond/completion-tests
  :description "Completion attachment, persistence, failure and concurrency tests"
  :depends-on (#:cl-jobpond/completions #:cl-jobpond/tests
               #:cl-jobpond/coordination-tests)
  :components ((:file "tests/completion-tests"))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call '#:cl-jobpond/tests '#:run-completion-tests)))
