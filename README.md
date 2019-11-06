# Distributed Evolving Context Tree (DECT)

DECT is a time-inhomogeneous variable-order Markov chain model introduced in our paper:
```
@inproceedings{shu:edbt:2016:dect,
    author = {Xiaokui Shu and Nikolay Laptev and Danfeng Yao},
    title = {{DECT}: Distributed Evolving Context Tree for Mining Web Behavior Evolution},
    booktitle = {Proceedings of the 19th International Conference on Extending Database Technology (EDBT)},
    month = {March},
    year = {2016},
    pages = {573--579},
    publisher = {OpenProceedings.org},
    address = {Konstanz, Germany},
    location = {Bordeaux, France},
}
```

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
  - Line example: "1433198382 site1:site3:site1:site1:site10:site25:site1:site99:site1"

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
