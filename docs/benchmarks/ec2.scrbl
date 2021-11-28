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
$ ssh -i "xxx.pem" ec2-user@ec2-18-208-228-89.compute-1.amazonaws.com

The authenticity of host 'ec2-18-208-228-89.compute-1.amazonaws.com (18.208.228.89)' can't be established.
ECDSA key fingerprint is SHA256:/MHCQjLdRmG6AubJo366zoYxYoPWfT0Ie2daqVJSgGM.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added 'ec2-18-208-228-89.compute-1.amazonaws.com,18.208.228.89' (ECDSA) to the list of known hosts.

       __|  __|_  )
       _|  (     /   Amazon Linux 2 AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-2/


[ec2-user@ip-172-31-33-4 ~]$ 
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
[ec2-user@ip-172-31-33-4 ~]$ sudo curl -L "https://github.com/docker/compose/releases/download/v2.2.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   664  100   664    0     0   8623      0 --:--:-- --:--:-- --:--:--  8623
100 23.5M  100 23.5M    0     0  44.5M      0 --:--:-- --:--:-- --:--:-- 70.3M

[ec2-user@ip-172-31-33-4 ~]$ sudo chmod +x /usr/local/bin/docker-compose
[ec2-user@ip-172-31-33-4 ~]$ sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
[ec2-user@ip-172-31-33-4 ~]$ docker-compose --version
Docker Compose version v2.2.0
}

When you've done all that, you can choose to append those commands to your user data (without sudo) as well. 
Now you've got docker-compose as a tool available inside EC2!

@section{How To Pull Docker Images From Amazon ECR}

You'll probably try is to pull a Docker image from Amazon ECR. If you don't configure anything, it will fail. 
There are two things you need to fix to make that work.

@itemlist{#:style 'numbered'
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
}

After that, you successfully set up an EC2 Amazon Machine Image with Docker, docker-compose, and access to Amazon ECR.
