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
                             (:file "conditions"))))
  :in-order-to ((asdf:test-op (asdf:test-op #:cl-jobpond/tests))))
