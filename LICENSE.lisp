;; Configurable Open Lisp License (COLL)
;;
;; You may copy and modify this template. Modified legal terms should not be
;; represented as the original COLL. Use of this template is at your own risk.

(defparameter *license*
  '((copyright . "Copyright 2026 Lambda Symbolics OÜ")
    (permissions . (use copy modify distribute sell sublicense))
    (conditions  . (retain-notice))
    (limitations . (no-warranty no-liability))))

(defun terms (kind)
  (cdr (assoc kind *license*)))

(defun license ()
  "Return an informational name followed by the authoritative license data."
  (let ((permissions (terms 'permissions))
        (conditions  (terms 'conditions)))
    (acons
     'variant
     (cond
       ((subsetp '(same-license disclose-source) conditions)
        "COLL-Copyleft")
       ((member 'same-license conditions)
        "COLL-ShareAlike")
       ((not (member 'distribute permissions))
        "COLL-NoDistribution")
       ((not (member 'sell permissions))
        "COLL-NoPaidDistribution")
       ((member 'retain-notice conditions)
        "COLL-Attribution")
       (t
        "COLL-Permissive"))
     *license*)))

;; LEGAL TERMS
;;
;; The literal value of *license* is authoritative. Evaluation is optional.
;; The variant returned by (license) is descriptive and has no legal effect.
;;
;; "Software" means the software and documentation carrying this license.
;; Active terms are the symbols listed in *license*. Unlisted terms do not apply.
;;
;; Subject to the active conditions, the copyright holder grants everyone a
;; worldwide, royalty-free, non-exclusive, irrevocable license to exercise each
;; active permission for the duration of the applicable copyright.
;;
;; PERMISSIONS
;; [use]          Use the Software for any purpose, commercial or otherwise.
;; [copy]         Make copies of the Software.
;; [modify]       Modify the Software and create derivative works.
;; [distribute]   Distribute the Software or derivative works.
;; [sell]         Sell copies of the Software or charge for their distribution.
;; [sublicense]   Grant others the active permissions.
;; [patent-grant] Exercise patent claims held by the copyright holder that are
;;                necessarily infringed by exercising the active permissions.
;;
;; CONDITIONS
;; [retain-notice]    Include this complete license and copyright notice in
;;                    every copy or derivative work you distribute.
;; [disclose-source]  When distributing compiled or transformed Software, make
;;                    its corresponding source code available to recipients.
;; [same-license]     Distribute derivative works under the same active terms.
;; [document-changes] Clearly identify significant modifications and their date.
;; [network-use]      Providing remote network access to modified Software counts
;;                    as distribution for the active conditions.
;;
;; LIMITATIONS
;; [no-warranty]  TO THE MAXIMUM EXTENT PERMITTED BY LAW, THE SOFTWARE IS
;;                PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;;                IMPLIED, INCLUDING MERCHANTABILITY, FITNESS FOR A PARTICULAR
;;                PURPOSE, AND NON-INFRINGEMENT.
;;
;; [no-liability] TO THE MAXIMUM EXTENT PERMITTED BY LAW, THE COPYRIGHT HOLDER
;;                SHALL NOT BE LIABLE FOR ANY CLAIM, LOSS, OR DAMAGE ARISING
;;                FROM THE SOFTWARE OR ANY DEALINGS IN THE SOFTWARE.
;;
;; [no-trademark] No trademark rights are granted, except as necessary to
;;                reproduce the required notices.
