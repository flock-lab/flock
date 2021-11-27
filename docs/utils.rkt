#lang racket
(provide exercise float-right panopto-vid shell shell-result)
(require scribble/base scribble/core scribble/html-properties
         redex/pict)

(define exercise-body-style
  (make-style "ExerciseBody" null))

(define exercise-style
  (make-style "Exercise" null))

(define *count* 0)

(define (exercise title . t)
  (set! *count* (add1 *count*))
  (nested #:style exercise-body-style
          (para #:style exercise-style (format "Exercise ~a: " *count*) title)
          t))


(define float-right
  (style #f (list (attributes '((style . "float: right"))))))

;; Embed a public panopto video into page
(define (panopto-vid src)
  (elem #:style
        (style #f (list (alt-tag "iframe")
                        (attributes
                          `((src . ,src)
                            (width . "688")
                            (height . "387")
                            (gesture . "media")
                            (allowfullscreen . "")
                            (style . "padding: 0px; border: 1px solid #464646;")))))))

;; calls proc and produces stdout appendde w/ stderr as a string
(define (with-output-to-string/err proc)
  (define os "")
  (define es "")
  (define r #f)
  (set! os (call-with-output-string
            (lambda (o)
              (set! es
                    (call-with-output-string
                     (λ (e)
                       (parameterize ([current-output-port o]
                                      [current-error-port e])
                         (set! r (proc)))))))))
  (unless r (error (string-append os es)))
  (string-append os es))

(define (shell-result c)
  (with-output-to-string/err (λ () (system #:set-pwd? #t c))))

(define (shell . cs)
  (match cs
    ['() ""]
    [(cons c cs)
     (string-append "> " c "\n"
                    (with-output-to-string/err (λ () (system #:set-pwd? #t c)))
                    (apply shell cs))]))

; dealing with unicode capable font issues
(metafunction-style "STIX Two Math")
