(ns merlion.core-test
  (:require [me.raynes.conch.low-level :as shell]
            [clojure.java.io :as io]
            [clojure.test :as test :refer [deftest testing is]]
            ))

(defn socat [opts]
  (let [p (shell/proc "/usr/bin/env" "socat"
                      "-u" "-d" "-d"
                      (:source opts)
                      "tcp-listen:0")]
    (let [e (io/reader (:err p))]
      (loop [lines (line-seq e)]
        (let [l (first lines)]
          (if-let [m  (re-find #"N listening on AF=2 (.+):([0-9]+)" l)]
            (assoc p :port (Integer/parseInt (last m)))
            (recur (rest lines))))))))

#_ (socat {:source "file:README.md"})
