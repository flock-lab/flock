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

@title[#:tag "flink_nexmark" #:style 'unnumbered]{Nexmark Benchmark for Apache Flink}

@section{What is Nexmark}

Nexmark is a benchmark suite for queries over continuous data streams. This benchmark is inspired by
the @link["https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/"]{NEXMark research paper} and Apache Beam.

These are multiple queries over a three entities model representing on online auction system:

@itemlist[
  @item{@bold{Person} represents a person submitting an item for auction and/or making a bid on an auction.}
  @item{@bold{Auction} represents an item under auction.}
  @item{@bold{Bid} represents a bid for an item under auction.}
]

@section{The Queries}

@tabular[#:style 'boxed
         #:row-properties '(bottom-border ())
         #:sep @hspace[2]
	 (list* (list @bold{Nexmark Benchmark Suite} 'cont 'cont)
	        (list @elem{@bold{Query}} @elem{@bold{Name}} @elem{@bold{Summary}})
            (list
            (list "q0"	"Pass Through"	"Measures the monitoring overhead including the source generator.")
            (list "q1"	"Currency Conversion"	"Convert each bid value from dollars to euros.")
            (list "q2"	"Selection"	"Find bids with specific auction ids and show their bid price.")
            (list "q3"	"Local Item Suggestion"	"Who is selling in OR, ID or CA in category 10, and for what auction ids?")
            (list "q4"	"Average Price for a Category"	"Select the average of the wining bid prices for all auctions in each category.")
            (list "q5"	"Hot Items"	"Which auctions have seen the most bids in the last period?")
            (list "q6"	"Average Selling Price by Seller"	"What is the average selling price per seller for their last 10 closed auctions.")
            (list "q7"	"Highest Bid"	"Select the bids with the highest bid price in the last period.")
            (list "q8"	"Monitor New Users"	"Select people who have entered the system and created auctions in the last period.")
            (list "q9"	"Winning Bids"	"Find the winning bid for each auction.")
            (list "q10"	"Log to File System"	"Log all events to file system. Illustrates windows streaming data into partitioned file system.")
            (list "q11"	"User Sessions"	"How many bids did a user make in each session they were active? Illustrates session windows.")
            (list "q12"	"Processing Time Windows"	"How many bids does a user make within a fixed processing time limit? Illustrates working in processing time window.")
            (list "q13"	"Bounded Side Input Join"	"Joins a stream to a bounded side input, modeling basic stream enrichment.")

            ))]

@emph{q1 ~ q8 are from original NEXMark queries, q9 ~ q13 are from Apache Beam.}

@section{Benchmark Guideline}

The Nexmark benchmark framework runs Flink queries on standalone cluster (Session Mode). The cluster should consist of
one master node and one or more worker nodes. All of them should be Linux environment (the CPU monitor script requries to
run on Linux). Please make sure you have the following software installed on each node:

@itemlist[#:style 'unnumbered
    @item{JDK 1.8.x or higher (Nexmark scripts uses some tools of JDK)}
    @item{ssh (sshd must be running to use the Flink and Nexmark scripts that manage remote components)}
]

Having @link["https://linuxize.com/post/how-to-setup-passwordless-ssh-login/"](passwordless SSH) and the same directory structure
on all your cluster nodes will allow you to use our scripts to control everything.

@subsection{Build Nexmark}

Before start to run the benchmark, you should build the Nexmark benchmark first to have a benchmark package.
Please make sure you have installed maven in your build machine. And run the emph{./build.sh} command under nexmark-flink directoy.
Then you will get the nexmark-flink.tgz archive under the directory.

@bash-repl{
$ git clone https://github.com/nexmark/nexmark
$ cd nexmark-flink
[nexmark-flink ~] mvn package -DskipTests
[nexmark-flink ~] ls target/
nexmark-flink-0.2-SNAPSHOT.jar  original-nexmark-flink-0.2-SNAPSHOT.jar
[nexmark-flink ~] ls
build.sh*  nexmark-flink.tgz  pom.xml  src/  target/
}

@subsection{Setup Flink Cluster}

@itemlist[#:style 'ordered
    @item{Download the latest Flink package from the download page.
    @bash-repl{
      $ wget https://dlcdn.apache.org/flink/flink-1.13.3/flink-1.13.3-bin-scala_2.12.tgz --no-check-certificate
      $ tar xzf flink-1.13.3-bin-scala_2.12.tgz; tar xzf nexmark-flink.tgz
      $ mv flink-1.13.3 flink; mv nexmark-flink nexmark
    }
    }
    @item{Copy the jars under @emph{nexmark/lib} to @emph{flink/lib} which contains the Nexmark source generator.}
    @item{Configure Flink
    @itemlist[#:style 'unnumbered
        @item{Edit @emph{flink/conf/workers} and enters the IP address of each worker node. Recommand to set 8 entries.}
        @item{Replace @emph{flink/conf/sql-client-defaults.yaml} by @emph{nexmark/conf/sql-client-defaults.yaml}}
        @item{Replace @emph{flink/conf/flink-conf.yaml} by @emph{nexmark/conf/flink-conf.yaml}. Remember to update the following configurations:
          @itemlist[#:style 'unnumbered
            @item{Set @emph{jobmanager.rpc.address} to you master IP address}
            @item{Set @emph{state.checkpoints.dir} to your local file path (recommend to use SSD), e.g. @emph{file:///home/username/checkpoint}.}
            @item{Set @emph{state.backend.rocksdb.localdir} to your local file path (recommend to use SSD), e.g. @emph{/home/username/rocksdb}.}
          ] 
        }

    ]
    }
    @item{Configure Nexmark benchmark. Set @emph{nexmark.metric.reporter.host} in @emph{nexmark/conf/nexmark.yaml} to your master IP address.}
    @item{Copy @emph{flink} and @emph{nexmark} folders to your worker nodes using @emph{scp}.}
    @item{Start Flink Cluster by running @emph{flink/bin/start-cluster.sh} on the master node.}
    @item{Start Nexmark benchmark by running @emph{nexmark/bin/setup_cluster.sh} on the master node.}
]


@subsection{Run Nexmark Benchmark}

You can run the Nexmark benchmark by running @emph{nexmark/bin/run_query.sh all} on the master node. 
It will run all the queries one by one, and collect benchmark metrics automatically. It will take 50 minutes 
to finish the benchmark by default. At last, it will print the benchmark summary result (Cores * Time(s) 
for each query) on the console. You can also run specific queries by running the following command:

@centered{nexmark/bin/run_query.sh q1,q2}

You can also tune the workload of the queries by editing @emph{nexmark/conf/nexmark.yaml} with the 
@emph{nexmark.workload.*} prefix options.

@section[#:style 'unnumbered]{References}

@itemlist[#:style 'ordered
  @item{Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier. NEXMark – A Benchmark for Queries over Data Streams. June 2010.}
  @item{Apache Beam, @link["https://beam.apache.org/"]{https://beam.apache.org/}}
  @item{Nexmark, @link["https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/"]{https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/}}
  @item{Nexmark Benchmark Suite, @link["https://github.com/nexmark/nexmark"]{https://github.com/nexmark/nexmark}}
]
