(ns hbase-clj.core-test
  (:require 
    [midje.sweet :refer :all]
    (hbase-clj
      [core :refer :all]
      [driver :refer :all]
      [manage :refer :all])))

(defhbase test-hbase 
  "hbase.zookeeper.quorum" "localhost")

(def table-name "hbase_clj_test_stu")

(create-table! 
  test-hbase table-name
  "info" "score")

(def-hbtable test-table
  {:id-type :string 
   :table-name table-name
   :hbase test-hbase}
  :info {:--ktype :keyword
         :--vtype :int
         :name :string}
  :score {:--ktype :keyword
        :--vtype :long})

(try 
  (with-table test-table
    (fact "inserting a row"
      (put-data! 
        "101" {:info {:age 17 :name "Alice"}
               :score {:math 99 :physics 78}}

        "102" {:info {:age 19 :name "Bob"}
               :score {:math 63 :physics 52}})
      ))
  (catch Exception e 
    (clojure.stacktrace/print-cause-trace e)))

(delete-table! test-hbase table-name)
