(defproject org.cluxis.hbase-clj "0.1.0-SNAPSHOT"
  :description "Talk to hbase in clojure"
  :url ""
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies [[midje "1.7.0"]]}
             :midje {}}
  :plugins [[lein-codox "0.9.4"]]
  :codox {:source-uri "https://github.com/cluXiss/hbase-clj/blob/master/{filepath}#L{line}"
          :output-path "./"
          :namespaces [hbase-clj.core hbase-clj.manage hbase-clj.codec]}
  :dependencies 
  [[org.clojure/clojure "1.8.0"]
   [org.apache.hbase/hbase-shaded-client "1.2.0"]])
