# Change Log
All notable changes to this project will be documented in this file. 
This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [0.1.0-alpha.2]
### New Features
* Added batch operation support on tables

### Bugfixes
* Solved mis-typematching problem on converting numeric values to bytes, see: [this question](http://stackoverflow.com/questions/36619496/clojure-calling-bytes-from-hbase-utils-returns-a-non-type-matching-result) and [this questiong](http://stackoverflow.com/questions/12586881/clojure-overloaded-method-resolution-for-longs)

## 0.1.0-alpha.1
### New Features
* Added gen-hbase, gen-htable functions
* Added ByteUtils
