(ns hbase-clj.core-test
  (:require 
    [midje.sweet :refer :all]
    (hbase-clj
      [core :refer :all]
      [driver :refer :all]
      [manage :refer :all])))

(defhbase test-hbase 
  "hbase.zookeeper.quorum" "localhost")

(create-table! 
  test-hbase "hbase_clj_test"
  "test1" "test2")

(delete-table! 
  test-hbase "hbase_clj_test")
