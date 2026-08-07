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
                             (:file "tests"))))
  :perform (asdf:test-op (operation component)
             (declare (ignore operation component))
             (uiop:symbol-call '#:cl-jobpond/tests '#:run-tests)))
