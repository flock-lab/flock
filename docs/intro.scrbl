#lang scribble/manual
@(require scribble/core
          "defns.rkt")

@title[#:style 'unnumbered]{Introduction}

@local-table-of-contents[]

@section{AWS Lambda}

@link["https://aws.amazon.com/lambda"]{AWS Lambda} is a compute service that lets users run code with- out provisioning or managing servers.
After uploading application code as a ZIP file or container image, Lambda automatically and precisely allocates
compute execution power on a high-availability compute infrastructure and runs application code based on the
incoming request or event, for any scale of traffic. When using Lambda, customers are responsible only for
their code. Lambda manages the compute fleet that offers a balance of memory, CPU, network, and other resources.

With AWS Lambda, users are charged based on the number of requests for their functions and the duration,
the time it takes for application code to execute. Lambda counts a request each time it starts executing in response
to an event notification or invoke call. Duration is calculated from the time user code begins executing until it returns
 or otherwise terminates, rounded up to the nearest 1 ms.
