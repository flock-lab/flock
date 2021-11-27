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

