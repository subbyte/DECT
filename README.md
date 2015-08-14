# DECT
Distributed Evolving Context Tree (DECT)

DECT is a realization of a Time-inhomogeneous Variable-order Markov Chain Model.

Project goal: model Internet user behavior change through time.
  - Markov model for recording Internet user behavior
  - Higher-order Markov model for precise behavior presentation
  - Variable-order Markov model is a succinct presentation of higher-order Markov model
  - Time-inhomogeneous Markov model tracks user behavior change through time

Technical highlights:
  - Flattened Context Tree
  - Session Batching
  - Parallel Window Operation
  - Parallel Pruning

Test Platform:
  - Spark 1.3.1
  - Scala 2.10.4
  - sbt 0.13.8

Input data format:
  - text file, single or distributed
  - each line is a user session
  - line format: timestamp \t siteID:siteID:siteID:siteID
  - timestamp should be UNIX time (seconds)
  - Line example: "1433198382 1:3:1:1:10:25:1:99:1"

Compile and run:
  - $ sbt package
  - modify the job-submission script template "run.sh.template" to run
  - find arguments details in src/main/scala/Dect.scala

Project dependencies:
  - https://github.com/scallop/scallop 
      - Copyright (C) 2012 Platon Pronko and Chris Hodapp
      - Licensed under under the MIT license
  - https://github.com/typesafehub/config
      - Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
      - Licensed under the Apache 2.0 license

Code licensed under the MIT license. See LICENSE file for terms.
