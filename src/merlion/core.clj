(ns merlion.core
  (:require [merlion.etcd :as etcd])
  (:gen-class))

(defn update-frontend [_ _ _ new-state]
  ;; note this is called on the agent thread so it'd be good if it doesn't block
  (println "new configuration" new-state))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (add-watch etcd/configuration :update-frontend update-frontend)
  (etcd/watch-config (first args))
  (println "running"))
