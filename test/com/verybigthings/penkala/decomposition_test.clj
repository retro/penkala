(ns com.verybigthings.penkala.decomposition-test
  (:require [clojure.test :refer [deftest is]]
            [com.verybigthings.penkala.decomposition :as d]
            [clojure.string :as str]))


(deftest it-should-return-nil-if-given-empty-data-and-schema
  (is (nil? (d/decompose {} []))))

(deftest it-should-collapse-simple-tree-structures
  (let [data [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
              {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]]
    (is (= [{:id 1 :val "p1" :children [{:id 11 :val "c1"} {:id 12 :val "c2"}]}]
           (d/decompose
            {:pk :parent_id
             :schema {:id :parent_id
                      :val :parent_val
                      :children {:pk :children_id
                                 :schema {:id :children_id :val :children_val}}}}
            data)))))

(deftest it-should-collapse-simple-tree-structures-ns
  (let [data [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
              {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]]
    (is (= [{:parent/id 1 :parent/val "p1" :parent/children [{:child/id 11 :child/val "c1"} {:child/id 12 :child/val "c2"}]}]
           (d/decompose
            {:pk :parent_id
             :namespace :parent
             :schema {:id :parent_id
                      :val :parent_val
                      :children {:pk :children_id
                                 :namespace :child
                                 :schema {:id :children_id :val :children_val}}}}
            data)))))

(deftest it-should-decompose-to-parent
  (let [data [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}]]
    (is (= [{:parent/id 1 :parent/val "p1" :child/id 11 :child/val "c1"}]
           (d/decompose
            {:pk :parent_id
             :namespace :parent
             :schema {:id :parent_id
                      :val :parent_val
                      :children {:pk :children_id
                                 :decompose-to :parent
                                 :namespace :child
                                 :schema {:id :children_id :val :children_val}}}}
            data)))))

(deftest it-should-handle-objects
  (is (= [{:id 1 :val "p1" :children [{:id 11 :val "c1"}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :children {:pk :children_id
                               :schema {:id :children_id :val :children_val}}}}
          {:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}))))

(deftest it-should-be-able-to-decompose-to-parent-nested-joins
  (let [data   [{:id 1 :username "retro"
                 :friendships__id 1
                 :friendships__source_id 1
                 :friendships__target_id 2
                 :friendships__target__id 2
                 :friendships__target__username "tiborkr"}
                {:id 1 :username "retro"
                 :friendships__id 1
                 :friendships__source_id 1
                 :friendships__target_id 3
                 :friendships__target__id 3
                 :friendships__target__username "dpoljak"}]
        schema {:pk :id
                :namespace :users
                :decompose-to :map
                :schema {:id :id
                         :username :username
                         :friendships {:pk [:friendships__source_id :friendships__target_id]
                                       :namespace :friendships
                                       :schema {:source_id :friendships__source_id
                                                :target_id :friendships__target_id
                                                :target {:pk :friendships__target__id
                                                         :namespace :users
                                                         :decompose-to :parent
                                                         :schema {:id :friendships__target__id
                                                                  :username :friendships__target__username}}}}}}]
    (is (= {:users/id 1
            :users/username "retro"
            :users/friendships [{:friendships/source_id 1
                                 :friendships/target_id 2
                                 :users/id 2
                                 :users/username "tiborkr"}
                                {:friendships/source_id 1
                                 :friendships/target_id 3
                                 :users/id 3
                                 :users/username "dpoljak"}]}
           (d/decompose schema data)))))

(deftest it-should-decompose-partial-results
  (is (= [{:id 1 :children [{:id 11}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :children {:pk :children_id
                               :schema {:id :children_id :val :children_val}}}}
          {:parent_id 1 :children_id 11}))))

(deftest it-should-handle-vector-fields
  (is (= [{:id 1 :arr ["one" "two"] :children [{:id 11 :arr ["three" "four"]}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :arr :parent_arr
                    :children {:pk :children_id
                               :schema {:id :children_id :arr :children_arr}}}}
          {:parent_id 1
           :parent_arr ["one" "two"]
           :children_id 11
           :children_arr ["three" "four"]}))))

(deftest can-use-vector-of-column-names-if-no-mapping-is-needed
  (is (= [{:parent_id 1 :parent_val "p1" :children [{:children_id 11 :children_val "c1"}
                                                    {:children_id 12 :children_val "c2"}]}]
         (d/decompose
          {:pk :parent_id
           :schema [:parent_id
                    :parent_val
                    {:children {:pk :children_id
                                :schema [:children_id :children_val]}}]}
          [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
           {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]))))

(deftest it-should-collapse-multiple-children-with-the-same-parent
  (is (= [{:id 1
           :val "p1"
           :children1 [{:id 11 :val "c1"} {:id 12 :val "c2"}]
           :children2 [{:id 21 :val "d1"} {:id 22 :val "d2"} {:id 23 :val "d3"}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :children1 {:pk :children1_id
                                :schema {:id :children1_id :val :children1_val}}
                    :children2 {:pk :children2_id
                                :schema {:id :children2_id :val :children2_val}}}}
          [{:parent_id 1 :parent_val "p1" :children1_id 11 :children1_val "c1" :children2_id 21 :children2_val "d1"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children2_id 22 :children2_val "d2"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children2_id 23 :children2_val "d3"}]))))

(deftest it-should-collapse-children-into-other-children
  (is (= [{:id 1
           :val "p1"
           :children1 [{:id 11
                        :val "c1"
                        :children2 [{:id 21 :val "d1"}]}
                       {:id 12
                        :val "c2"
                        :children2 [{:id 22 :val "d2"}
                                    {:id 23 :val "d3"}]}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :children1 {:pk :children1_id
                                :schema {:id :children1_id
                                         :val :children1_val
                                         :children2 {:pk :children1_children2_id
                                                     :schema {:id :children1_children2_id
                                                              :val :children1_children2_val}}}}}}
          [{:parent_id 1 :parent_val "p1" :children1_id 11 :children1_val "c1" :children1_children2_id 21 :children1_children2_val "d1"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children1_children2_id 22 :children1_children2_val "d2"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children1_children2_id 23 :children1_children2_val "d3"}]))))

(deftest it-should-collapse-object-descendants
  (is (= [{:id 1 :val "p1" :child {:id 11 :val "c1" :grandchild {:id 111 :val "g1"}}}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :child {:pk :child_id
                            :decompose-to :map
                            :schema {:id :child_id
                                     :val :child_val
                                     :grandchild {:pk :grandchild_id
                                                  :schema {:id :grandchild_id
                                                           :val :grandchild_val}
                                                  :decompose-to :map}}}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest decomposes-to-indexed-by-pk
  (is (= [{:id 1 :val "p1" :child {11 {:id 11 :val "c1" :grandchild {111 {:id 111 :val "g1"}}}}}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :child {:pk :child_id
                            :decompose-to :indexed-by-pk
                            :schema {:id :child_id
                                     :val :child_val
                                     :grandchild {:pk :grandchild_id
                                                  :schema {:id :grandchild_id
                                                           :val :grandchild_val}
                                                  :decompose-to :indexed-by-pk}}}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest decomposes-to-indexed-by-pk-with-array-column-list
  (is (= [{:id 1 :val "p1" :child {11 {:child_id 11 :child_val "c1" :grandchild {111 {:id 111 :val "g1"}}}}}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :child {:pk :child_id
                            :decompose-to :indexed-by-pk
                            :schema [:child_id
                                     :child_val
                                     {:grandchild {:pk :grandchild_id
                                                   :schema {:id :grandchild_id
                                                            :val :grandchild_val}
                                                   :decompose-to :indexed-by-pk}}]}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest consolidates-duplicate-children-by-pk
  (is (= [{:id 1 :val "p1" :children [{:child_id 11 :val "c1"} {:child_id 12 :val "c2"}]}]
         (d/decompose
          {:pk :parent_id
           :schema {:id :parent_id
                    :val :parent_val
                    :children {:pk :children_child_id
                               :schema {:child_id :children_child_id
                                        :val :children_val}}}}
          [{:parent_id 1 :parent_val "p1" :children_child_id 11 :children_val "c1"}
           {:parent_id 1 :parent_val "p1" :children_child_id 12 :children_val "c2"}
           {:parent_id 1 :parent_val "p1" :children_child_id 12 :children_val "c2"}]))))

(deftest it-should-apply-new-parents-only-in-the-correct-scope
  (is (= [{:id 1
           :account {:id 1}
           :name "Eduardo Luiz"
           :contact {:email "email", :phone "phone"}
           :notes nil
           :archived false
           :address
           {:zipCode "zip"
            :street "street"
            :number "number"
            :complement nil
            :neighborhood nil
            :city "Sao Paulo"
            :state "Sao Paulo"
            :coords {:latitude "1", :longitude "2"}}
           :labels
           [{:id "297726d0-301d-4de6-b9a4-e439b81f44ba"
             :name "Contrato"
             :color "yellow"
             :type 1}
            {:id "1db6e07f-91e2-42fb-b65c-9a364b6bad4c"
             :name "Particular"
             :color "purple"
             :type 1}]}]
         (d/decompose
          {:pk :this_id
           :schema {:id :this_id
                    :name :this_name
                    :notes :this_notes
                    :archived :this_archived
                    :account {:pk :account_id
                              :decompose-to :map
                              :schema {:id :account_id}}
                    :contact {:pk :this_id
                              :decompose-to :map
                              :schema {:email :contact_email
                                       :phone :contact_phone}}
                    :address {:pk :this_id
                              :decompose-to :map
                              :schema {:number :address_number
                                       :street :address_street
                                       :complement :address_complement
                                       :neighborhood :address_neighborhood
                                       :city :address_city
                                       :state :address_state
                                       :zipCode :address_zipCode
                                       :coords {:pk :this_id
                                                :decompose-to :map
                                                :schema {:latitude :address_coords_latitude
                                                         :longitude :address_coords_longitude}}}}
                    :labels {:pk :labels_id
                             :schema {:id :labels_id
                                      :name :labels_name
                                      :color :labels_color
                                      :type :labels_type}}}}
          [{:address_state "Sao Paulo"
            :this_archived false
            :contact_email "email"
            :address_zipCode "zip"
            :address_number "number"
            :account_id 1
            :labels_id
            "297726d0-301d-4de6-b9a4-e439b81f44ba"
            :this_notes nil
            :address_neighborhood nil
            :this_id 1
            :labels_type 1
            :contact_phone "phone"
            :labels_name "Contrato"
            :address_coords_latitude "1"
            :address_complement nil
            :labels_color "yellow"
            :address_street "street"
            :address_coords_longitude "2"
            :address_city "Sao Paulo"
            :this_name "Eduardo Luiz"}
           {:address_state "Sao Paulo"
            :this_archived false
            :contact_email "email"
            :address_zipCode "zip"
            :address_number "number"
            :account_id 1
            :labels_id
            "1db6e07f-91e2-42fb-b65c-9a364b6bad4c"
            :this_notes nil
            :address_neighborhood nil
            :this_id 1
            :labels_type 1
            :contact_phone "phone"
            :labels_name "Particular"
            :address_coords_latitude "1"
            :address_complement nil
            :labels_color "purple"
            :address_street "street"
            :address_coords_longitude "2"
            :address_city "Sao Paulo"
            :this_name "Eduardo Luiz"}]))))

(deftest it-should-accept-and-use-pk-vectors
  (is (= [{:id_one 1
           :id_two 2
           :val "p1"
           :children1
           [{:id_one 11
             :id_two 12
             :val "c1"
             :children2
             [{:id_one 21, :id_two 22, :val "d1"}]}
            {:id_one 13
             :id_two 14
             :val "c2"
             :children2
             [{:id_one 23, :id_two 24, :val "d2"}
              {:id_one 25, :id_two 26, :val "d3"}]}]}
          {:id_one 3
           :id_two 4
           :val "p2"
           :children1
           [{:id_one 15
             :id_two 16
             :val "c3"
             :children2
             [{:id_one 27, :id_two 28, :val "d4"}]}]}]
         (d/decompose
          {:pk [:parent_id_one :id_one :parent_id_two :parent_id_two]
           :schema {:id_one :parent_id_one
                    :id_two :parent_id_two
                    :val :parent_val
                    :children1 {:pk [:children1_id_one :children1_id_two]
                                :schema {:id_one :children1_id_one
                                         :id_two :children1_id_two
                                         :val :children1_val
                                         :children2 {:pk [:children1_children2_id_one :children1_children2_id_two]
                                                     :schema {:id_one :children1_children2_id_one
                                                              :id_two :children1_children2_id_two
                                                              :val :children1_children2_val}}}}}}
          [{:children1_children2_id_one 21
            :children1_id_two 12
            :children1_id_one 11
            :parent_id_one 1
            :children1_children2_id_two 22
            :parent_val "p1"
            :children1_val "c1"
            :parent_id_two 2
            :children1_children2_val "d1"}
           {:children1_children2_id_one 23
            :children1_id_two 14
            :children1_id_one 13
            :parent_id_one 1
            :children1_children2_id_two 24
            :parent_val "p1"
            :children1_val "c2"
            :parent_id_two 2
            :children1_children2_val "d2"}
           {:children1_children2_id_one 25
            :children1_id_two 14
            :children1_id_one 13
            :parent_id_one 1
            :children1_children2_id_two 26
            :parent_val "p1"
            :children1_val "c2"
            :parent_id_two 2
            :children1_children2_val "d3"}
           {:children1_children2_id_one 27
            :children1_id_two 16
            :children1_id_one 15
            :parent_id_one 3
            :children1_children2_id_two 28
            :parent_val "p2"
            :children1_val "c3"
            :parent_id_two 4
            :children1_children2_val "d4"}]))))

(deftest it-should-process-items-with-processor-1
  (let [data [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
              {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]]
    (is (= [{:id 1
             :val "p1"
             :children-count 2
             :children [{:id 11 :val "c1" :val-upcase "C1"}
                        {:id 12 :val "c2" :val-upcase "C2"}]}]
           (d/decompose
            {:pk :parent_id
             :processor (fn [val]
                          (assoc val :children-count (-> val :children count)))
             :schema {:id :parent_id
                      :val :parent_val
                      :children {:pk :children_id
                                 :processor (fn [val]
                                              (assoc val :val-upcase (-> val :val str/upper-case)))
                                 :schema {:id :children_id :val :children_val}}}}
            data)))))

(deftest it-should-process-items-with-processor-2
  (let [data [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}]]
    (is (= [{:parent/id 1
             :parent/val "p1"
             :parent/val-upcased "P1"
             :child/id 11
             :child/val "c1"
             :child/val-upcased "C1"}]
           (d/decompose
            {:pk :parent_id
             :namespace :parent
             :processor (fn [val]
                          (assoc val :parent/val-upcased (-> val :parent/val str/upper-case)))
             :schema {:id :parent_id
                      :val :parent_val
                      :children {:pk :children_id
                                 :decompose-to :parent
                                 :namespace :child
                                 :processor (fn [val]
                                              (assoc val :child/val-upcased (-> val :child/val str/upper-case)))
                                 :schema {:id :children_id :val :children_val}}}}
            data)))))

(deftest it-should-decompose-embedded-data
  (let [data [{:beta
               {:types
                {:val "text", :j "jsonb", :id "integer", :alpha-id "integer"},
                :data
                [{:val "alpha three", :j nil, :id 3, :alpha-id 3}
                 {:val "alpha three again", :j nil, :id 4, :alpha-id 3}]},
               :id 3,
               :val "three"}]
        decomposed (d/decompose {:pk :id
                                 :namespace :alpha
                                 :schema {:id :id
                                          :val :val
                                          :beta {:embedded? true
                                                 :pk :id
                                                 :namespace :beta
                                                 :schema {:id :id
                                                          :val :val
                                                          :j :j
                                                          :alpha-id :alpha-id}}}}
                                data)]
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
           decomposed))))