(defproject merlion "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.1"]
                 [cheshire "5.6.3"]
                 [ring "1.5.0"]
                 [org.clojure/core.async "0.2.385"]
                 [ring/ring-codec "1.0.1"]]
  :plugins [[lein-ancient "0.6.10"]]
  :main ^:skip-aot merlion.core
  :target-path "target/%s"
  :profiles {:repl {:plugins [[cider/cider-nrepl "0.12.0"]]}
             :uberjar {:aot :all}})
