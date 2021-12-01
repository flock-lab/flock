#lang scribble/manual
@title[#:tag "flink_intro" #:style 'unnumbered]{Introduction to Apache Flink}

@link["https://flink.apache.org/"]{Apache Flink} is a framework and distributed processing engine for stateful
computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster
environments, perform computations at in-memory speed and at any scale.

Flink enables you to perform transformations on many different data sources,
such as Amazon Kinesis Streams or the Apache Cassandra database.  Flink has some SQL support for these stream and batch datasets.
Flink’s API is categorized into DataSets and DataStreams. DataSets are transformations on sets or collections of
distributed data, while DataStreams are transformations on streaming data like those found in Amazon Kinesis.

The Flink runtime consists of two different types of daemons: @bold{JobManagers}, which are responsible for
coordinating scheduling, checkpoint, and recovery functions, and @bold{TaskManagers}, which are the worker processes
that execute tasks and transfer data between streams in an application. Each application has one JobManager
and at least one TaskManager.


You can use the Docker images to deploy a @bold{Session} or @bold{Application} cluster on Docker.

@itemlist[#:style 'unnumbered

@item{@bold{Application Mode}: This is a lightweight and scalable way to submit an application on Flink and is the preferred way
to launch application as it supports better resource isolation. Resource isolation is achieved by running a cluster
per job. Once the application shuts down all the Flink components are cleaned up.}

@item{@bold{Session Mode}: This is a long running Kubernetes deployment of Flink. Multiple applications can be launched on a
cluster and the applications competes for the resources. There may be multiple jobs running on a TaskManager in
parallel. Its main advantage is that it saves time on spinning up a new Flink cluster for new jobs, however if one
of the Task Managers fails it may impact all the jobs running on that.}
]

Visit Flink's source code in a browser:

@centered{@link["https://github.com/apache/flink"]{https://github.com/apache/flink}}


For Flink, we have the following questions:

@itemlist[#:style 'ordered

@item{What is the performance and price difference between users deploying Flink to
@link["https://aws.amazon.com/ec2"]{AWS EC2} and directly using serverless services provided by cloud
service providers such as @link["https://aws.amazon.com/kinesis/data-analytics/"]{Amazon Kinesis Data Analytics}?}

@item{Under different machine configurations and the number of computing nodes, which solution is
more convenient, effective and affordable for users?}

@item{Compared with Flock, what are their advantages and disadvantages? What applications are most suitable for Flock's architecture?
}

]

In order to answer these questions, let's talk about how to deploy Flink on Amazon EC2 nodes and how to configure
Amazon Kinesis Data Analytics to run Flink jobs.
