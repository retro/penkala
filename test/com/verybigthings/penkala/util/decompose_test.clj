(ns com.verybigthings.penkala.util.decompose-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [com.verybigthings.penkala.util.decompose :as d]))

(s/check-asserts true)

(deftest it-should-return-nil-if-given-empty-data-and-schema
  (is (nil? (d/decompose {} []))))

(deftest it-should-collapse-simple-tree-structures
  (is (= [{:id 1 :val "p1" :children [{:id 11 :val "c1"} {:id 12 :val "c2"}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :children {:pk :children_id
                                :columns {:children_id :id :children_val :val}}}}
          [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
           {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]))))

(deftest it-should-handle-objects
  (is (= [{:id 1 :val "p1" :children [{:id 11 :val "c1"}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :children {:pk :children_id
                                :columns {:children_id :id :children_val :val}}}}
          {:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}))))

(deftest it-should-decompose-partial-results
  (is (= [{:id 1 :children [{:id 11}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :children {:pk :children_id
                                :columns {:children_id :id :children_val :val}}}}
          {:parent_id 1 :children_id 11}))))

(deftest it-should-handle-array-fields
  (is (= [{:id 1 :arr ["one" "two"] :children [{:id 11 :arr ["three" "four"]}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_arr :arr
                     :children {:pk :children_id
                                :columns {:children_id :id :children_arr :arr}}}}
          {:parent_id 1
           :parent_arr ["one" "two"]
           :children_id 11
           :children_arr ["three" "four"]}))))

(deftest can-use-arrays-of-column-names-if-no-mapping-is-needed
  (is (= [{:parent_id 1 :parent_val "p1" :children [{:children_id 11 :children_val "c1"}
                                                    {:children_id 12 :children_val "c2"}]}]
        (d/decompose
          {:pk :parent_id
           :columns [:parent_id
                     :parent_val
                     {:children {:pk :children_id
                                 :columns [:children_id :children_val]}}]}
          [{:parent_id 1 :parent_val "p1" :children_id 11 :children_val "c1"}
           {:parent_id 1 :parent_val "p1" :children_id 12 :children_val "c2"}]))))

(deftest it-should-collapse-multiple-children-with-the-same-parent
  (is (= [{:id 1
           :val "p1"
           :children1 [{:id 11 :val "c1"} {:id 12 :val "c2"}]
           :children2 [{:id 21 :val "d1"} {:id 22 :val "d2"} {:id 23 :val "d3"}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :children1 {:pk :children1_id
                                 :columns {:children1_id :id :children1_val :val}}
                     :children2 {:pk :children2_id
                                 :columns {:children2_id :id :children2_val :val}}}}
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
           :columns {:parent_id :id
                     :parent_val :val
                     :children1 {:pk :children1_id
                                 :columns {:children1_id :id
                                           :children1_val :val
                                           :children2 {:pk :children1_children2_id
                                                       :columns {:children1_children2_id :id
                                                                 :children1_children2_val :val}}}}}}
          [{:parent_id 1 :parent_val "p1" :children1_id 11 :children1_val "c1" :children1_children2_id 21 :children1_children2_val "d1"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children1_children2_id 22 :children1_children2_val "d2"}
           {:parent_id 1 :parent_val "p1" :children1_id 12 :children1_val "c2" :children1_children2_id 23 :children1_children2_val "d3"}]))))

(deftest it-should-collapse-object-descendants
  (is (= [{:id 1 :val "p1" :child {:id 11 :val "c1" :grandchild {:id 111 :val "g1"}}}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :child {:pk :child_id
                             :decompose-to :map
                             :columns {:child_id :id
                                       :child_val :val
                                       :grandchild {:pk :grandchild_id
                                                    :columns {:grandchild_id :id
                                                              :grandchild_val :val}
                                                    :decompose-to :map}}}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest decomposes-to-dictionaries
  (is (= [{:id 1 :val "p1" :child {11 {:id 11 :val "c1" :grandchild {111 {:id 111 :val "g1"}}}}}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :child {:pk :child_id
                             :decompose-to :dict
                             :columns {:child_id :id
                                       :child_val :val
                                       :grandchild {:pk :grandchild_id
                                                    :columns {:grandchild_id :id
                                                              :grandchild_val :val}
                                                    :decompose-to :dict}}}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest decomposes-to-dictionaries-with-array-column-list
  (is (= [{:id 1 :val "p1" :child {11 {:child_id 11 :child_val "c1" :grandchild {111 {:id 111 :val "g1"}}}}}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :child {:pk :child_id
                             :decompose-to :dict
                             :columns [:child_id
                                       :child_val
                                       {:grandchild {:pk :grandchild_id
                                                     :columns {:grandchild_id :id
                                                               :grandchild_val :val}
                                                     :decompose-to :dict}}]}}}
          {:parent_id 1 :parent_val "p1" :child_id 11 :child_val "c1" :grandchild_id 111 :grandchild_val "g1"}))))

(deftest consolidates-duplicate-children-by-pk
  (is (= [{:id 1 :val "p1" :children [{:child_id 11 :val "c1"} {:child_id 12 :val "c2"}]}]
        (d/decompose
          {:pk :parent_id
           :columns {:parent_id :id
                     :parent_val :val
                     :children {:pk :children_child_id
                                :columns {:children_child_id :child_id
                                          :children_val :val}}}}
          [{:parent_id 1 :parent_val "p1" :children_child_id 11 :children_val "c1"}
           {:parent_id 1 :parent_val "p1" :children_child_id 12 :children_val "c2"}
           {:parent_id 1 :parent_val "p1" :children_child_id 12 :children_val "c2"}]))))

(deftest it-should-apply-new-parents-only-in-the-correct-scope
  (is (= [{:id 1,
           :account {:id 1},
           :name "Eduardo Luiz",
           :contact {:email "email", :phone "phone"},
           :notes nil,
           :archived false,
           :address
           {:zipCode "zip",
            :street "street",
            :number "number",
            :complement nil,
            :neighborhood nil,
            :city "Sao Paulo",
            :state "Sao Paulo",
            :coords {:latitude "1", :longitude "2"}},
           :labels
           [{:id "297726d0-301d-4de6-b9a4-e439b81f44ba",
             :name "Contrato",
             :color "yellow",
             :type 1}
            {:id "1db6e07f-91e2-42fb-b65c-9a364b6bad4c",
             :name "Particular",
             :color "purple",
             :type 1}]}]
        (d/decompose
          {:pk :this_id,
           :columns {:this_id :id,
                     :this_name :name,
                     :this_notes :notes,
                     :this_archived :archived
                     :account {:pk :account_id,
                               :decompose-to :map
                               :columns {:account_id :id}}
                     :contact {:pk :this_id
                               :decompose-to :map
                               :columns {:contact_email :email
                                         :contact_phone :phone}}
                     :address {:pk :this_id
                               :decompose-to :map
                               :columns {:address_number :number
                                         :address_street :street
                                         :address_complement :complement
                                         :address_neighborhood :neighborhood
                                         :address_city :city
                                         :address_state :state
                                         :address_zipCode :zipCode
                                         :coords {:pk :this_id
                                                  :decompose-to :map
                                                  :columns {:address_coords_latitude :latitude
                                                            :address_coords_longitude :longitude}}}}
                     :labels {:pk :labels_id
                              :columns {:labels_id :id
                                        :labels_name :name
                                        :labels_color :color
                                        :labels_type :type}}}}
          [{:address_state "Sao Paulo",
            :this_archived false,
            :contact_email "email",
            :address_zipCode "zip",
            :address_number "number",
            :account_id 1,
            :labels_id
            "297726d0-301d-4de6-b9a4-e439b81f44ba",
            :this_notes nil,
            :address_neighborhood nil,
            :this_id 1,
            :labels_type 1,
            :contact_phone "phone",
            :labels_name "Contrato",
            :address_coords_latitude "1",
            :address_complement nil,
            :labels_color "yellow",
            :address_street "street",
            :address_coords_longitude "2",
            :address_city "Sao Paulo",
            :this_name "Eduardo Luiz"}
           {:address_state "Sao Paulo",
            :this_archived false,
            :contact_email "email",
            :address_zipCode "zip",
            :address_number "number",
            :account_id 1,
            :labels_id
            "1db6e07f-91e2-42fb-b65c-9a364b6bad4c",
            :this_notes nil,
            :address_neighborhood nil,
            :this_id 1,
            :labels_type 1,
            :contact_phone "phone",
            :labels_name "Particular",
            :address_coords_latitude "1",
            :address_complement nil,
            :labels_color "purple",
            :address_street "street",
            :address_coords_longitude "2",
            :address_city "Sao Paulo",
            :this_name "Eduardo Luiz"}]))))

(deftest it-should-accept-and-use-pk-vectors
  (is (= [{:id_one 1,
           :id_two 2,
           :val "p1",
           :children1
           [{:id_one 11,
             :id_two 12,
             :val "c1",
             :children2
             [{:id_one 21, :id_two 22, :val "d1"}]}
            {:id_one 13,
             :id_two 14,
             :val "c2",
             :children2
             [{:id_one 23, :id_two 24, :val "d2"}
              {:id_one 25, :id_two 26, :val "d3"}]}]}
          {:id_one 3,
           :id_two 4,
           :val "p2",
           :children1
           [{:id_one 15,
             :id_two 16,
             :val "c3",
             :children2
             [{:id_one 27, :id_two 28, :val "d4"}]}]}]
        (d/decompose
          {:pk [:parent_id_one :id_one :parent_id_two :parent_id_two]
           :columns {:parent_id_one :id_one
                     :parent_id_two :id_two
                     :parent_val :val
                     :children1 {:pk [:children1_id_one :children1_id_two]
                                 :columns {:children1_id_one :id_one
                                           :children1_id_two :id_two
                                           :children1_val :val
                                           :children2 {:pk [:children1_children2_id_one :children1_children2_id_two]
                                                       :columns {:children1_children2_id_one :id_one
                                                                 :children1_children2_id_two :id_two
                                                                 :children1_children2_val :val}}}}}}
          [{:children1_children2_id_one 21,
            :children1_id_two 12,
            :children1_id_one 11,
            :parent_id_one 1,
            :children1_children2_id_two 22,
            :parent_val "p1",
            :children1_val "c1",
            :parent_id_two 2,
            :children1_children2_val "d1"}
           {:children1_children2_id_one 23,
            :children1_id_two 14,
            :children1_id_one 13,
            :parent_id_one 1,
            :children1_children2_id_two 24,
            :parent_val "p1",
            :children1_val "c2",
            :parent_id_two 2,
            :children1_children2_val "d2"}
           {:children1_children2_id_one 25,
            :children1_id_two 14,
            :children1_id_one 13,
            :parent_id_one 1,
            :children1_children2_id_two 26,
            :parent_val "p1",
            :children1_val "c2",
            :parent_id_two 2,
            :children1_children2_val "d3"}
           {:children1_children2_id_one 27,
            :children1_id_two 16,
            :children1_id_one 15,
            :parent_id_one 3,
            :children1_children2_id_two 28,
            :parent_val "p2",
            :children1_val "c3",
            :parent_id_two 4,
            :children1_children2_val "d4"}]))))