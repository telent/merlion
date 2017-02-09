(defproject merlion "0.1.0-SNAPSHOT"
  :description "etcd-based TCP proxy"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [clj-http-lite "0.3.0"]
                 [cheshire "5.6.3"]
                 [ring "1.6.0-beta6"  :exclusions [org.clojure/java.classpath]]
                 [com.taoensso/timbre "4.8.0"]
                 [org.clojure/core.async "0.2.395"]
                 [org.clojure/test.check "0.9.0"]
                 [ring/ring-codec "1.0.1"]]
  :plugins [[lein-ancient "0.6.10"]]
  :main ^:skip-aot merlion.core
  :target-path "target/%s"
  :profiles {:repl {:plugins [[cider/cider-nrepl "0.12.0"]]}
             :dev {:dependencies [[me.raynes/conch "0.8.0"
                                   :exclusions [org.clojure/clojure]]]}
             :test {:dependencies [[me.raynes/conch "0.8.0"
                                    :exclusions [org.clojure/clojure]]]}
             :uberjar {:global-vars {*warn-on-reflection* true
                                     *assert* false}
                       :aot :all}})
