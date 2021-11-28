#lang racket
(provide (all-defined-out))
(require scribble/core scribble/html-properties scribble/manual)

(define dan (link "https://www.cs.umd.edu/~abadi/" "Daniel Abadi"))
(define dan-email "abadi@umd.edu")

(define amol (link "https://www.cs.umd.edu/~amol/" "Amol Deshpande"))
(define amol-email "amol@umd.edu")

(define gang (link "https://gangliao.me/" "Gang Liao"))
(define gang-email "gangliao@cs.umd.edu")

(define staff
  (list (list (link "http://jmct.cc/" "José Manuel Calderón Trilla") "jmct@umd.edu" "-")        
        (list "William Chung" "wchung1@terpmail.umd.edu" "Th 3:30-5:30 Online")
        (list "Justin Frank" "jpfrank@umd.edu" "W 12:00-2:00 AVW 4160")
        (list "Vyas Gupta" "vgupta13@terpmail.umd.edu" "F 1:30-3:30 AVW 4160")))

(define project "Flock")

(define racket-version "8.3")

(define discord "https://discord.gg/Cxzesghj")
