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

@title[#:tag "flink_ec2" #:style 'unnumbered]{Flink on EC2}

@table-of-contents[]

@section{Install Docker in EC2 Instance}

In this section, we will describe how to deploy a Flink cluster on Amazon EC2. We use a @bold{r4.8xlarge} instance 
to run the Flink cluster. Each instance has 32 threads and 244 GB of memory. 

The first step is to login to the EC2 instance. We use the @bold{ssh} command to login to the instance.

@bash-repl{
ssh -i "xxx.pem" ...compute-1.amazonaws.com

The authenticity of host 'ec2-18-208-228-89.compute-1.amazonaws.com (18.208.228.89)' can't be established.
ECDSA key fingerprint is SHA256:/MHCQjLdRmG6AubJo366zoYxYoPWfT0Ie2daqVJSgGM.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added 'ec2-18-208-228-89.compute-1.amazonaws.com,18.208.228.89' (ECDSA) to the list of known hosts.

       __|  __|_  )
       _|  (     /   Amazon Linux 2 AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-2/


[ec2-user ~]$ 
}

Then, we need to install Docker on the instance. We use the @bold{sudo} command to install Docker.

@bash-repl{
sudo yum update -y
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
}

But there is one more subtle detail. Every time your Amazon AMI is rebooted, you want the Docker service to remain 
up and running. Therefore, we have one final command to use:

@bash-repl{
sudo systemctl enable docker
}

After this, we can check that the Docker service is running.

@bash-repl{
sudo docker ps

CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
}


@section{Install Docker-Compose in EC2 Instance}

@bold{docker-compose} is a very convenient tool to work with. If you have a few Docker images, you quickly get 
tired of entering everything via the command line. This is where docker-compose comes in. It allows you to 
configure all the images in one place. An application consisting of multiple containers can easily be started 
with the command @bold{docker-compose up -d} and stopped with @bold{docker-compose down}.

You can install Docker-Compose using the following commands:

@bash-repl{
[ec2-user ~]$ sudo curl -L "https://github.com/docker/compose/releases/download/v2.2.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   664  100   664    0     0   8623      0 --:--:-- --:--:-- --:--:--  8623
100 23.5M  100 23.5M    0     0  44.5M      0 --:--:-- --:--:-- --:--:-- 70.3M

[ec2-user ~]$ sudo chmod +x /usr/local/bin/docker-compose
[ec2-user ~]$ sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
[ec2-user ~]$ docker-compose --version
Docker Compose version v2.2.0
}

When you've done all that, you can choose to append those commands to your user data (without sudo) as well. 
Now you've got docker-compose as a tool available inside EC2!

@section{How To Pull Docker Images From Amazon ECR}

You'll probably try is to pull a Docker image from Amazon ECR. If you don't configure anything, it will fail. 
There are two things you need to fix to make that work.

@itemlist[#:style 'ordered
  @item{
    Select an IAM role. We'll need at least read access to ECR to pull Docker images from a private repository.
  }
  @item{
    You need to log into AWS inside your EC2 container.

    @bash-repl{
      $ aws ecr get-login-password --region us-east-1 | docker login --username AWS \
        --password-stdin XXXXXX.dkr.ecr.us-east-1.amazonaws.com
    }
  }
]

@margin-note{After that, you successfully set up an EC2 Amazon Machine Image with Docker, docker-compose, and access to Amazon ECR.}

@section{How To Run Flink on EC2}

The next step is to run Flink on EC2. We'll use the @bold{flink-1.13.3-scala_2.12-java11} image. To deploy a Flink Session cluster with Docker, 
you need to start a JobManager container. To enable communication between the containers, we first set a required 
Flink configuration property and create a network:

@bash-repl{
export FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
sudo docker network create flink-network
}

Then we launch the JobManager:

@bash-repl{
docker run \
    --rm \
    --name=jobmanager \
    --network flink-network \
    --publish 8081:8081 \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    flink:latest jobmanager & 
}

and one or more TaskManager containers:

@bash-repl{
docker run \
    --rm \
    --name=taskmanager \
    --network flink-network \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    flink:latest taskmanager &   
}

@bash-repl{
[ec2-user ~]$ sudo docker ps
CONTAINER ID   IMAGE          COMMAND                  CREATED          STATUS          PORTS                                                 NAMES
4c2e97bcedc6   flink:latest   "/docker-entrypoint.…"   11 minutes ago   Up 11 minutes   6123/tcp, 8081/tcp                                    taskmanager
9f590618dcb9   flink:latest   "/docker-entrypoint.…"   12 minutes ago   Up 12 minutes   6123/tcp, 0.0.0.0:8081->8081/tcp, :::8081->8081/tcp   jobmanager
}

The web interface is now available at localhost:8081.

@image[#:scale 1/2]{img/benchmarks/flink-ui.png}
The Flink web interface

To shut down the cluster, either terminate (e.g. with CTRL-C) the JobManager and TaskManager processes, or 
use @bold{docker ps} to identify and @bold{docker stop} to terminate the containers.
