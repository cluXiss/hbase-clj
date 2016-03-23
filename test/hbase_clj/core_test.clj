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

;;Delete the test table first to avoid accidents
(try 
  (delete-table! test-hbase table-name)
  (catch Exception e nil))

(create-table! 
  test-hbase table-name
  "info" "score")

(def-hbtable test-table
  {:id-type :string 
   :table-name table-name
   :hbase test-hbase}
  :info  {:--ktype :keyword  :--vtype :int :name :string}
  :score {:--ktype :keyword  :--vtype :long})

(try 
  (fact "About getting and putting"
    (with-table test-table
      (put-data! 
        ["101" {:info {:age 17 :name "Alice"}
                :score {:math 99 :physics 78}}]

        ["102" {:info {:age 19 :name "Bob"}
                :score {:math 63 :physics 52}}])

      (get-data "102") 
      => (just [["102"
                 {:info {:age 19 :name "Bob"}
                  :score {:math 63 :physics 52}}]])

      (get-data :with-versions "102")
      => (just 
           [(just 
              ["102"
               (contains 
                 {:info 
                  (contains 
                    {:age (contains [(contains {:val 19})])})})])])

      (get-data ["101" [:info]])
      => (just [(just ["101" (just {:info (contains {})})])])

      (get-data ["101" [[:score [:math]]]])
      => (just [(just ["101" (just {:score (just {:math number?})})])])
      
      ))

  (catch Exception e 
    (clojure.stacktrace/print-cause-trace e)))

(delete-table! test-hbase table-name)
