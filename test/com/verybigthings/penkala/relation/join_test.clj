(ns com.verybigthings.penkala.relation.join-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.db :refer [select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :once (partial th/reset-db-fixture "foreign-keys"))

(deftest it-joins-a-relation-with-a-type-and-keys
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        alpha-beta (-> alpha
                     (r/join :inner beta :beta [:= :id :beta/alpha-id])
                     (r/where [:= :id 3]))
        res (select! *env* alpha-beta)]
    (is (= [{:alpha/id 3
             :alpha/val "three"
             :alpha/beta [{:beta/id 3
                           :beta/alpha-id 3
                           :beta/j nil
                           :beta/val "alpha three"}
                          {:beta/id 4
                           :beta/alpha-id 3
                           :beta/j nil
                           :beta/val "alpha three again"}]}]
          res))))

(deftest it-joins-a-view-with-an-explicit-pk-1
  (let [alpha (:alpha *env*)
        beta-view (-> *env* :beta-view (r/with-pk [:id]))
        alpha-beta-view (-> alpha
                          (r/join :inner beta-view :beta-view [:= :id :beta-view/alpha-id])
                          (r/where [:= :id 3]))
        res (select! *env* alpha-beta-view)]
    (is (= [#:alpha{:id 3
                    :val "three"
                    :beta-view [#:beta-view{:id 3 :alpha-id 3 :j nil :val "alpha three"}
                                #:beta-view{:id 4 :alpha-id 3 :j nil :val "alpha three again"}]}]
          res))))

(deftest it-joins-a-view-with-an-explicit-pk-2
  (let [alpha (:alpha *env*)
        beta-view (:beta-view *env*)
        alpha-beta-view (-> alpha
                          (r/join :inner beta-view :beta-view [:= :id :beta-view/alpha-id])
                          (r/where [:= :id 3]))
        res (select! *env* alpha-beta-view {} {:columns {:beta-view {:pk ["id"]}}})]
    (is (= [#:alpha{:id 3
                    :val "three"
                    :beta-view [#:beta-view{:id 3 :alpha-id 3 :j nil :val "alpha three"}
                                #:beta-view{:id 4 :alpha-id 3 :j nil :val "alpha three again"}]}]
          res))))

(deftest it-joins-from-a-view-with-an-explicit-pk-1
  (let [beta-view (-> *env* :beta-view (r/with-pk [:id]))
        alpha (:alpha *env*)
        alpha-beta-view (-> beta-view
                          (r/with-pk [:id])
                          (r/join :inner alpha :alpha [:= :alpha-id :alpha/id])
                          (r/where [:= :alpha/id 3]))
        res (select! *env* alpha-beta-view)]
    (is (= [{:beta-view/alpha [{:alpha/id 3, :alpha/val "three"}],
             :beta-view/alpha-id 3,
             :beta-view/id 3,
             :beta-view/j nil,
             :beta-view/val "alpha three"}
            {:beta-view/alpha [{:alpha/id 3, :alpha/val "three"}],
             :beta-view/alpha-id 3,
             :beta-view/id 4,
             :beta-view/j nil,
             :beta-view/val "alpha three again"}]
          res))))

(deftest it-joins-from-a-view-with-an-explicit-pk-2
  (let [beta-view (:beta-view *env*)
        alpha (:alpha *env*)
        alpha-beta-view (-> beta-view
                          (r/join :inner alpha :alpha [:= :alpha-id :alpha/id])
                          (r/where [:= :alpha/id 3]))
        res (select! *env* alpha-beta-view {} {:pk [:id]})]
    (is (= [{:beta-view/alpha [{:alpha/id 3, :alpha/val "three"}],
             :beta-view/alpha-id 3,
             :beta-view/id 3,
             :beta-view/j nil,
             :beta-view/val "alpha three"}
            {:beta-view/alpha [{:alpha/id 3, :alpha/val "three"}],
             :beta-view/alpha-id 3,
             :beta-view/id 4,
             :beta-view/j nil,
             :beta-view/val "alpha three again"}]
          res))))

(deftest it-joins-a-relation-in-another-schema
  (let [alpha (:alpha *env*)
        epsilon (:sch/epsilon *env*)
        alpha-epsilon (-> alpha
                        (r/join :inner epsilon :epsilon [:= :id :epsilon/alpha-id]))
        res (select! *env* alpha-epsilon)]
    (is (= [{:alpha/epsilon [{:epsilon/alpha-id 1 :epsilon/id 1 :epsilon/val "alpha one"}]
             :alpha/id 1
             :alpha/val "one"}]
          res))))

(deftest it-joins-multiple-tables-at-multiple-levels
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        sch-delta (:sch/delta *env*)
        sch-epsilon (:sch/epsilon *env*)
        joined (-> alpha
                 (r/join :inner
                   (-> beta
                     (r/join :inner gamma :gamma [:= :id :gamma/beta-id])
                     (r/join :inner sch-delta :delta [:= :id :delta/beta-id]))
                   :beta
                   [:= :id :beta/alpha-id])
                 (r/join :inner sch-epsilon :epsilon [:= :id :epsilon/alpha-id]))
        res (select! *env* joined)]
    (is (= [#:alpha{:val "one"
                    :id 1
                    :beta
                    [#:beta{:val "alpha one"
                            :j nil
                            :id 1
                            :alpha-id 1
                            :gamma
                            [#:gamma{:beta-id 1
                                     :alpha-id-one 1
                                     :alpha-id-two 1
                                     :val "alpha one alpha one beta one"
                                     :j nil
                                     :id 1}]
                            :delta
                            [#:delta{:beta-id 1 :val "beta one" :id 1}]}],
                    :epsilon [#:epsilon{:val "alpha one" :id 1 :alpha-id 1}]}]
          res))))

(deftest it-can-join-on-any-fields
  (let [beta (:beta *env*)
        sch-epsilon (:sch/epsilon *env*)
        beta-sch-epsilon (-> beta
                           (r/join :inner sch-epsilon :epsilon [:= :val :epsilon/val]))
        res (select! *env* beta-sch-epsilon)]
    (is (= [{:beta/alpha-id 1,
             :beta/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :beta/id 1,
             :beta/j nil,
             :beta/val "alpha one"}]
          res))))

(deftest it-can-join-on-constants
  (let [beta (:beta *env*)
        sch-epsilon (:sch/epsilon *env*)
        beta-sch-epsilon (-> beta
                           (r/join :inner sch-epsilon :epsilon [:= :epsilon/val "alpha one"])
                           (r/where [:= :val "alpha three again"]))
        res (select! *env* beta-sch-epsilon)]
    (is (= [{:beta/alpha-id 3,
             :beta/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :beta/id 4,
             :beta/j nil,
             :beta/val "alpha three again"}]
          res))))

(deftest it-can-join-on-multiple-constants
  (let [beta (:beta *env*)
        sch-epsilon (:sch/epsilon *env*)
        beta-sch-epsilon (-> beta
                           (r/join :inner sch-epsilon :epsilon
                             [:and [:= :epsilon/id 1] [:= :epsilon/val "alpha one"]])
                           (r/where [:= :val "alpha three again"]))
        res (select! *env* beta-sch-epsilon)]
    (is (= [{:beta/alpha-id 3,
             :beta/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :beta/id 4,
             :beta/j nil,
             :beta/val "alpha three again"}]
          res))))

(deftest it-can-join-on-constants-for-multiple-relations
  (let [beta (:beta *env*)
        alpha (:alpha *env*)
        sch-epsilon (:sch/epsilon *env*)
        beta-sch-epsilon (-> beta
                           (r/join :inner alpha :alpha [:= :alpha/val "one"])
                           (r/join :inner sch-epsilon :epsilon
                             [:and [:= :epsilon/id 1] [:= :epsilon/val "alpha one"]])
                           (r/where [:= :val "alpha three again"]))
        res (select! *env* beta-sch-epsilon)]
    (is (= [{:beta/alpha-id 3,
             :beta/alpha [{:alpha/id 1 :alpha/val "one"}]
             :beta/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :beta/id 4,
             :beta/j nil,
             :beta/val "alpha three again"}]
          res))))

(deftest it-can-join-on-columns-and-constants
  (let [alpha (:alpha *env*)
        sch-epsilon (:sch/epsilon *env*)
        alpha-sch-epsilon (-> alpha
                            (r/join :inner sch-epsilon :epsilon [:and [:= :epsilon/val "alpha one"]
                                                                 [:= :id :epsilon/alpha-id]]))
        res (select! *env* alpha-sch-epsilon)]
    (is (= [{:alpha/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :alpha/id 1,
             :alpha/val "one"}]
          res))))

(deftest it-can-join-on-complex-predicates-1
  (let [alpha (:alpha *env*)
        sch-epsilon (:sch/epsilon *env*)
        alpha-sch-epsilon  (-> alpha
                             (r/join :inner sch-epsilon :epsilon [:is-null :epsilon/alpha-id])
                             (r/where [:= :val "one"]))
        res (select! *env* alpha-sch-epsilon)]
    (is (= [{:alpha/epsilon [{:epsilon/alpha-id nil, :epsilon/id 2, :epsilon/val "not two"}],
             :alpha/id 1,
             :alpha/val "one"}]
          res))))

(deftest it-can-join-on-complex-predicates-2
  (let [alpha (:alpha *env*)
        sch-epsilon (:sch/epsilon *env*)
        alpha-sch-epsilon  (-> alpha
                             (r/join :inner sch-epsilon :epsilon [:in :epsilon/alpha-id [1 2]])
                             (r/where [:= :val "one"]))
        res (select! *env* alpha-sch-epsilon)]
    (is (= [{:alpha/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}],
             :alpha/id 1,
             :alpha/val "one"}]
          res))))

(deftest it-can-join-on-complex-predicates-4
  (let [beta (:beta *env*)
        gamma (:gamma *env*)
        beta-gamma (-> beta
                     (r/join :inner gamma :gamma [:= ["#>>" :j ["x" "y"]] ["#>>" :gamma/j ["z" "a"]]])
                     (r/where [:= :val "not five"]))
        res (select! *env* beta-gamma)]
    (is (= [{:beta/alpha-id nil,
             :beta/gamma [{:gamma/alpha-id-one 4,
                           :gamma/alpha-id-two nil,
                           :gamma/beta-id 5,
                           :gamma/id 6,
                           :gamma/j {:z {:a "test"}},
                           :gamma/val "beta five"}],
             :beta/id 6,
             :beta/j {:x {:y "test"}},
             :beta/val "not five"}]
          res))))

(deftest it-can-left-join
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        alpha-beta (-> alpha
                     (r/join :left beta :beta [:= :id :beta/alpha-id])
                     (r/where [:> :id 1]))
        res (select! *env* alpha-beta)]
    (is (= [{:alpha/beta [{:beta/alpha-id 2, :beta/id 2, :beta/j nil, :beta/val "alpha two"}],
             :alpha/id 2,
             :alpha/val "two"}
            {:alpha/beta [{:beta/alpha-id 3, :beta/id 3, :beta/j nil, :beta/val "alpha three"}
                          {:beta/alpha-id 3, :beta/id 4, :beta/j nil, :beta/val "alpha three again"}],
             :alpha/id 3,
             :alpha/val "three"}
            {:alpha/beta [], :alpha/id 4, :alpha/val "four"}]
          res))))

(deftest it-can-join-on-true
  (let [alpha (:alpha *env*)
        epsilon (:sch/epsilon *env*)
        alpha-epsilon (-> alpha
                        (r/join :inner epsilon :epsilon true)
                        (r/where [:= :id 1]))
        res (select! *env* alpha-epsilon)]
    (is (= [{:alpha/epsilon [{:epsilon/alpha-id 1, :epsilon/id 1, :epsilon/val "alpha one"}
                             {:epsilon/alpha-id nil, :epsilon/id 2, :epsilon/val "not two"}],
             :alpha/id 1,
             :alpha/val "one"}]
          res))))

(deftest it-decomposes-multiple-records
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        alpha-beta (-> alpha
                     (r/join :inner beta :beta [:= :id :beta/alpha-id])
                     (r/where [:> :id 1]))
        res (select! *env* alpha-beta)]
    (is (= [{:alpha/beta [{:beta/alpha-id 2, :beta/id 2, :beta/j nil, :beta/val "alpha two"}],
             :alpha/id 2,
             :alpha/val "two"}
            {:alpha/beta [{:beta/alpha-id 3, :beta/id 3, :beta/j nil, :beta/val "alpha three"}
                          {:beta/alpha-id 3, :beta/id 4, :beta/j nil, :beta/val "alpha three again"}],
             :alpha/id 3,
             :alpha/val "three"}]
          res))))

(deftest it-joins-to-a-joined-relation-instead-of-the-origin
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        joined (-> alpha
                 (r/join
                   :inner
                   (r/join beta :inner gamma :gamma [:= :id :gamma/beta-id])
                   :beta [:= :id :beta/alpha-id])
                 (r/where [:> :id 1]))
        res (select! *env* joined)]
    (is (= [{:alpha/beta [{:beta/alpha-id 2,
                           :beta/gamma [{:gamma/alpha-id-one 1,
                                         :gamma/alpha-id-two 2,
                                         :gamma/beta-id 2,
                                         :gamma/id 2,
                                         :gamma/j nil,
                                         :gamma/val "alpha two alpha two beta two"}
                                        {:gamma/alpha-id-one 2,
                                         :gamma/alpha-id-two 3,
                                         :gamma/beta-id 2,
                                         :gamma/id 3,
                                         :gamma/j nil,
                                         :gamma/val "alpha two alpha three beta two again"}],
                           :beta/id 2,
                           :beta/j nil,
                           :beta/val "alpha two"}],
             :alpha/id 2,
             :alpha/val "two"}
            {:alpha/beta [{:beta/alpha-id 3,
                           :beta/gamma [{:gamma/alpha-id-one 2,
                                         :gamma/alpha-id-two nil,
                                         :gamma/beta-id 3,
                                         :gamma/id 4,
                                         :gamma/j nil,
                                         :gamma/val "alpha two (alpha null) beta three"}],
                           :beta/id 3,
                           :beta/j nil,
                           :beta/val "alpha three"}
                          {:beta/alpha-id 3,
                           :beta/gamma [{:gamma/alpha-id-one 3,
                                         :gamma/alpha-id-two 1,
                                         :gamma/beta-id 4,
                                         :gamma/id 5,
                                         :gamma/j nil,
                                         :gamma/val "alpha three alpha one beta four"}],
                           :beta/id 4,
                           :beta/j nil,
                           :beta/val "alpha three again"}],
             :alpha/id 3,
             :alpha/val "three"}]
          res))))

(deftest it-omits-relation-from-the-final-result
  (let [alpha (:alpha *env*)
        alpha-zeta (r/with-pk (:alpha-zeta *env*) [:alpha-id :zeta-id])
        zeta (:zeta *env*)
        joined (-> alpha
                 (r/join :left alpha-zeta :alpha-zeta [:= :id :alpha-zeta/alpha-id])
                 (r/join :left zeta :zeta [:= :alpha-zeta/zeta-id :zeta/id])
                 (r/where [:in :id [1 3]]))
        res (select! *env* joined {} {:columns {:alpha-zeta {:decompose-to :omit}}})]
    (is (= [{:alpha/id 1,
             :alpha/val "one",
             :alpha/zeta [{:zeta/id 1, :zeta/val "alpha one"}
                          {:zeta/id 2, :zeta/val "alpha one again"}]}
            {:alpha/id 3, :alpha/val "three", :alpha/zeta []}]
          res))))

(deftest it-omits-a-parent-relation-from-the-final-result
  (let [alpha (:alpha *env*)
        alpha-zeta (r/with-pk (:alpha-zeta *env*) [:alpha-id :zeta-id])
        zeta (:zeta *env*)
        joined (-> alpha
                 (r/join :left
                   (-> alpha-zeta
                     (r/join :left zeta :zeta [:= :zeta-id :zeta/id]))
                   :alpha-zeta [:= :id :alpha-zeta/alpha-id])
                 (r/where [:in :id [1 3]]))
        res (select! *env* joined {} {:columns {:alpha-zeta {:decompose-to :omit}}})]
    (is (= [{:alpha/id 1,
             :alpha/val "one",
             :alpha/zeta [{:zeta/id 1, :zeta/val "alpha one"}
                          {:zeta/id 2, :zeta/val "alpha one again"}]}
            {:alpha/id 3, :alpha/val "three", :alpha/zeta []}]
          res))))

(deftest it-correctly-namespaces-columns-when-not-decomposing-1
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        joined (-> alpha
                 (r/join
                   :inner
                   (r/join beta :inner gamma :gamma [:= :id :gamma/beta-id])
                   :beta [:= :id :beta/alpha-id])
                 (r/where [:> :id 1]))
        res (select! *env* joined {} false)]
    (is (= [{:id 2,
             :val "two",
             :beta/alpha-id 2,
             :beta/id 2,
             :beta/j nil,
             :beta/val "alpha two",
             :beta.gamma/alpha-id-one 1,
             :beta.gamma/alpha-id-two 2,
             :beta.gamma/beta-id 2,
             :beta.gamma/id 2,
             :beta.gamma/j nil,
             :beta.gamma/val "alpha two alpha two beta two"}
            {:id 2,
             :val "two",
             :beta/alpha-id 2,
             :beta/id 2,
             :beta/j nil,
             :beta/val "alpha two",
             :beta.gamma/alpha-id-one 2,
             :beta.gamma/alpha-id-two 3,
             :beta.gamma/beta-id 2,
             :beta.gamma/id 3,
             :beta.gamma/j nil,
             :beta.gamma/val "alpha two alpha three beta two again"}
            {:id 3,
             :val "three",
             :beta/alpha-id 3,
             :beta/id 3,
             :beta/j nil,
             :beta/val "alpha three",
             :beta.gamma/alpha-id-one 2,
             :beta.gamma/alpha-id-two nil,
             :beta.gamma/beta-id 3,
             :beta.gamma/id 4,
             :beta.gamma/j nil,
             :beta.gamma/val "alpha two (alpha null) beta three"}
            {:id 3,
             :val "three",
             :beta/alpha-id 3,
             :beta/id 4,
             :beta/j nil,
             :beta/val "alpha three again",
             :beta.gamma/alpha-id-one 3,
             :beta.gamma/alpha-id-two 1,
             :beta.gamma/beta-id 4,
             :beta.gamma/id 5,
             :beta.gamma/j nil,
             :beta.gamma/val "alpha three alpha one beta four"}]
          res))))

(deftest it-correctly-namespaces-columns-when-not-decomposing-2
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        joined (-> alpha
                 (r/join
                   :inner
                   (r/join beta :inner gamma :gamma [:= :id :gamma/beta-id])
                   :beta [:= :id :beta/alpha-id])
                 (r/where [:> :id 1]))
        res (select! *env* joined {} {:columns {:beta false}})]
    (is (= [{:alpha/beta [{:beta/alpha-id 2,
                           :beta/id 2,
                           :beta/j nil,
                           :beta/val "alpha two",
                           :beta.gamma/alpha-id-one 1,
                           :beta.gamma/alpha-id-two 2,
                           :beta.gamma/beta-id 2,
                           :beta.gamma/id 2,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two alpha two beta two"}
                          {:beta/alpha-id 2,
                           :beta/id 2,
                           :beta/j nil,
                           :beta/val "alpha two",
                           :beta.gamma/alpha-id-one 2,
                           :beta.gamma/alpha-id-two 3,
                           :beta.gamma/beta-id 2,
                           :beta.gamma/id 3,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two alpha three beta two again"}],
             :alpha/id 2,
             :alpha/val "two"}
            {:alpha/beta [{:beta/alpha-id 3,
                           :beta/id 3,
                           :beta/j nil,
                           :beta/val "alpha three",
                           :beta.gamma/alpha-id-one 2,
                           :beta.gamma/alpha-id-two nil,
                           :beta.gamma/beta-id 3,
                           :beta.gamma/id 4,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two (alpha null) beta three"}
                          {:beta/alpha-id 3,
                           :beta/id 4,
                           :beta/j nil,
                           :beta/val "alpha three again",
                           :beta.gamma/alpha-id-one 3,
                           :beta.gamma/alpha-id-two 1,
                           :beta.gamma/beta-id 4,
                           :beta.gamma/id 5,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha three alpha one beta four"}],
             :alpha/id 3,
             :alpha/val "three"}]
          res))))

(deftest it-correctly-namespaces-columns-when-not-using-inner-or-left-join
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        joined (-> alpha
                 (r/join
                   :right
                   (r/join beta :inner gamma :gamma [:= :id :gamma/beta-id])
                   :beta [:= :id :beta/alpha-id])
                 (r/where [:> :id 1]))
        res (select! *env* joined)]
    (is (= [{:alpha/beta [{:beta/alpha-id 2,
                           :beta/id 2,
                           :beta/j nil,
                           :beta/val "alpha two",
                           :beta.gamma/alpha-id-one 1,
                           :beta.gamma/alpha-id-two 2,
                           :beta.gamma/beta-id 2,
                           :beta.gamma/id 2,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two alpha two beta two"}
                          {:beta/alpha-id 2,
                           :beta/id 2,
                           :beta/j nil,
                           :beta/val "alpha two",
                           :beta.gamma/alpha-id-one 2,
                           :beta.gamma/alpha-id-two 3,
                           :beta.gamma/beta-id 2,
                           :beta.gamma/id 3,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two alpha three beta two again"}],
             :alpha/id 2,
             :alpha/val "two"}
            {:alpha/beta [{:beta/alpha-id 3,
                           :beta/id 3,
                           :beta/j nil,
                           :beta/val "alpha three",
                           :beta.gamma/alpha-id-one 2,
                           :beta.gamma/alpha-id-two nil,
                           :beta.gamma/beta-id 3,
                           :beta.gamma/id 4,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha two (alpha null) beta three"}
                          {:beta/alpha-id 3,
                           :beta/id 4,
                           :beta/j nil,
                           :beta/val "alpha three again",
                           :beta.gamma/alpha-id-one 3,
                           :beta.gamma/alpha-id-two 1,
                           :beta.gamma/beta-id 4,
                           :beta.gamma/id 5,
                           :beta.gamma/j nil,
                           :beta.gamma/val "alpha three alpha one beta four"}],
             :alpha/id 3,
             :alpha/val "three"}]
          res))))