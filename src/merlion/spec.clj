(ns merlion.spec
  (:require [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.spec :as s]))

;; We're doing something here a bit like 'designators' in the Common Lisp
;; standard - things which can be coerced into valid values for the
;; spec are acceptable, as is the value itself, and calling conform
;; should get the value.  I can't see a better way
;; to do this yet.

(defmacro coercer [target-spec conversion-fn]
  `(s/conformer #(if (s/valid? ~target-spec %)
                   %
                   (try (~conversion-fn %) (catch Exception e# :s/invalid)))))
