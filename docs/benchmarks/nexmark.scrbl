#lang scribble/manual
@(require scribble/core
	scriblib/footnote
    scribble/decode
    scribble/html-properties
    "../defns.rkt"
    "../utils.rkt")

@title[#:tag "flink_nexmark" #:style 'unnumbered]{Nexmark Benchmark for Apache Flink}

@section{What is Nexmark}

Nexmark is a benchmark suite for queries over continuous data streams. This benchmark is inspired by 
the @link["https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/"]{NEXMark research paper}.

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
