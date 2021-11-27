#lang scribble/manual
@(require "../fancyverb.rkt" "../utils.rkt")
@(require scribble/core racket/list)
@(require (for-label racket rackunit))
@(require redex/reduction-semantics
          redex/pict (only-in pict scale))

@(require scribble/examples racket/sandbox)

@(define core-racket
  (parameterize ([sandbox-output 'string]
                 [sandbox-error-output 'string]
                 [sandbox-memory-limit 50])
    (make-evaluator 'racket)))

@(core-racket '(require racket/match))

@(define (bash-repl . s)
  (filebox (emph "shell")
    (apply fancyverbatim "bash" s)))

@title[#:tag "flink_eks" #:style 'unnumbered]{Flink on Amazon EKS}

@table-of-contents[]

@section{Overview}

Flink supports different deployment modes when running on @link["https://kubernetes.io/"]{Kubernetes}.  We 
will show you how to deploy Flink on Kubernetes using the 
@link["https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/deployment/resource-providers/native_kubernetes/"]{Native Kubernetes Deployment}.

@section[#:style 'unnumbered]{Amazon EKS}

@link["https://aws.amazon.com/eks/"]{Amazon EKS} is a fully managed Kubernetes service. EKS supports creating and managing spot instances using Amazon EKS 
managed node groups following Spot best practices. This enables you to take advantage of the steep savings and scale
that Spot Instances provide for interruptible workloads. EKS-managed node groups require less operational effort 
compared to using self-managed nodes.

Flink can run jobs on Kubernetes via Application and Session Modes only:

@bold{Application Mode}: This is a lightweight and scalable way to submit an application on Flink and is the preferred way 
to launch application as it supports better resource isolation. Resource isolation is achieved by running a cluster 
per job. Once the application shuts down all the Flink components are cleaned up.

@bold{Session Mode}: This is a long running Kubernetes deployment of Flink. Multiple applications can be launched on a 
cluster and the applications competes for the resources. There may be multiple jobs running on a TaskManager in 
parallel. Its main advantage is that it saves time on spinning up a new Flink cluster for new jobs, however if one 
of the Task Managers fails it may impact all the jobs running on that.

@section[#:style 'unnumbered]{AWS Spot Instances}

Flink is distributed to manage and process high volumes of data. Designed for failure, they can run on machines with 
different configurations, inherently resilient and flexible. Spot Instances can optimize runtimes by increasing 
throughput, while spending the same (or less). Flink can tolerate interruptions using restart and failover strategies.

Job Manager and Task Manager are key building blocks of Flink. The Task Manager is the compute intensive part and 
Job Manager is the orchestrator. We would be running Task Manager on Spot Instances and Job Manager on On Demand 
Instances.

Flink supports elastic scaling via @link["https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/"]{Reactive Mode}. 
This is ideal with Spot Instances as it implements elastic scaling with higher throughput in a cost optimized way.

@section[#:style 'unnumbered]{Flink Deployment}

For production use, we recommend deploying Flink Applications in the @bold{Application Mode}, as these modes provide a 
better isolation for the Applications. We will be bundling the user code in the Flink image for that purpose and 
upload in @link["https://aws.amazon.com/ecr/"]{Amazon ECR}. Amazon ECR is a fully managed container registry that 
makes it easy to store, manage, share, and deploy your container images and artifacts anywhere.

@itemlist[#:style 'ordered
@item{Build the Amazon ECR Image
@itemlist[
@item{Login using the following cmd and don’t forget to replace the AWS_REGION and AWS_ACCOUNT_ID with your details.}
@item{Create a repository.}
@item{Build the Docker image.}
@item{Tag and Push your image to Amazon ECR.}
]}

@item{Create Amazon S3/Amazon Kinesis Access Policy
@itemlist[
@item{We must create an access policy to allow the Flink application to read/write from Amazon S3 and read Kinesis data streams.
Run the following to create the policy. Note the ARN.}
]}

@item{Cluster and node groups deployment
@itemlist[
@item{Create an EKS cluster. The cluster takes approximately 15 minutes to launch.}
@item{Create the node group using the nodeGroup config file. We are using multiple nodeGroups of different 
sizes to adapt Spot best practice of diversification.  Replace the <<Policy ARN>> string using the ARN string 
from the previous step.}
@item{Download the Cluster Autoscaler and edit it to add the cluster-name.}
]}

@item{Install the Cluster AutoScaler using the following command: kubectl apply -f cluster-autoscaler-autodiscover.yaml
@itemlist[
@item{Using EKS Managed node groups requires significantly less operational effort compared to using self-managed 
node group and enables: 1) Auto enforcement of Spot best practices. 2) Spot Instance lifecycle management. 3)
Auto labeling of Pods.}
@item{eksctl has integrated @link["https://github.com/aws/amazon-ec2-instance-selector"]{amazon-ec2-instance-selector}
to enable auto selection of instances based on the criteria passed.}
]}

@item{Create service accounts for Flink}

@bash-repl{
$ kubectl create serviceaccount flink-service-account
$ kubectl create clusterrolebinding flink-role-binding-flink \
--clusterrole=edit --serviceaccount=default:flink-service-account
}

@item{Deploy Flink

This install folder here has all the YAML files required to deploy a standalone Flink cluster. Run the install.sh 
file. This will deploy the cluster with a JobManager, a pool of TaskManagers and a Service exposing JobManager’s 
ports.

@itemlist[
@item{This is a High-Availability(HA) deployment of Flink with the use of Kubernetes high availability service.}
@item{The JobManager runs on OnDemand and TaskManager on Spot. As the cluster is launched in Application Mode, 
if a node is interrupted only one job will be restarted.}
@item{Autoscaling is enabled by the use of Reactive Mode. Horizontal Pod Autoscaler is used to monitor the CPU
load and scale accordingly.}
@item{Check-pointing is enabled which allows Flink to save state and be fault tolerant.}
]}

]


@section[#:style 'unnumbered]{Conclusion}

In this post, we demonstrated how you can run Flink workloads on a Kubernetes Cluster using Spot Instances, 
achieving scalability, resilience, and cost optimization. 


@section[#:style 'unnumbered]{Reference}

@itemlist[#:style 'ordered
@item{Kinnar Sen. Optimizing Apache Flink on Amazon EKS using Amazon EC2 Spot Instances. AWS Compute Blog. 11 NOV 2021. @link["https://aws.amazon.com/blogs/compute/optimizing-apache-flink-on-amazon-eks-using-amazon-ec2-spot-instances/"]{https://aws.amazon.com/blogs/compute/optimizing-apache-flink-on-amazon-eks-using-amazon-ec2-spot-instances/}.}
]
