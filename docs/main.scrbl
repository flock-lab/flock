#lang scribble/manual
@(require scribble/core
	  scriblib/footnote
          scribble/decode
          scribble/html-properties
      	 "defns.rkt"
          "utils.rkt")

@(define (blockquote . strs)
   (make-nested-flow (make-style "blockquote" '(command))
                     (decode-flow strs)))


@(define accessible
   (style #f (list (js-addition "js/accessibility.js")
                   (attributes '((lang . "en"))))))

@title[@elem[#:style 'bold]{Flock: A Serverless Streaming SQL Engine}]

@image[#:scale 1/4 #:style float-right]{img/flock.png}

Flock is a cloud-native SQL query engine for event-driven analytics on cloud function services.
One of the key benefits of serverless query engine is the ease in which it can scale to meet traffic
demands or requests, with little to no need for capacity planning.
Its major goal is to provide users with better price performance for stream processing on cloud.

@bold{Contributors}: @gang, @dan, @amol, Zejun Liu
@bold{Communications:} @link[@discord]{Discord}

@bold{Disclaimer:} All information on this web page is tentative and subject to
change.

@include-section{intro.scrbl}
