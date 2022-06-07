(ns com.verybigthings.penkala.postgresql-tutorial-test
  "This namespace implements test cases based on the queries found on
   https://www.postgresqltutorial.com/. You can follow the tutorials
   and see how are queries implemented with Penkala"
  (:require [clojure.test :refer [deftest testing use-fixtures]]
            [com.verybigthings.penkala.next-jdbc :refer [insert! select! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [testit.core :refer [fact facts => =in=>]]))

;;(use-fixtures :each (partial th/reset-db-fixture "pagila"))
(use-fixtures :once th/pagila-db-fixture th/instrument-penkala)

(defn date? [val]
  (= java.time.LocalDate (class val)))

(defn datetime? [val]
  (= java.time.LocalDateTime (class val)))

(deftest select
  ;; https://www.postgresqltutorial.com/postgresql-select/
  (testing "Query data from one column"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:first-name "MARY"}
                  #:customer{:first-name "PATRICIA"}
                  #:customer{:first-name "LINDA"}
                  #:customer{:first-name "BARBARA"}
                  #:customer{:first-name "ELIZABETH"}
                  #:customer{:first-name "JENNIFER"}
                  #:customer{:first-name "MARIA"}
                  #:customer{:first-name "SUSAN"}
                  #:customer{:first-name "MARGARET"}
                  #:customer{:first-name "DOROTHY"}])))

  (testing "Query data from multiple columns"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name :email]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:email "MARY.SMITH@sakilacustomer.org"
                             :last-name "SMITH"
                             :first-name "MARY"}
                  #:customer{:email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :last-name "JOHNSON"
                             :first-name "PATRICIA"}
                  #:customer{:email "LINDA.WILLIAMS@sakilacustomer.org"
                             :last-name "WILLIAMS"
                             :first-name "LINDA"}
                  #:customer{:email "BARBARA.JONES@sakilacustomer.org"
                             :last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:email "ELIZABETH.BROWN@sakilacustomer.org"
                             :last-name "BROWN"
                             :first-name "ELIZABETH"}
                  #:customer{:email "JENNIFER.DAVIS@sakilacustomer.org"
                             :last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:email "MARIA.MILLER@sakilacustomer.org"
                             :last-name "MILLER"
                             :first-name "MARIA"}
                  #:customer{:email "SUSAN.WILSON@sakilacustomer.org"
                             :last-name "WILSON"
                             :first-name "SUSAN"}
                  #:customer{:email "MARGARET.MOORE@sakilacustomer.org"
                             :last-name "MOORE"
                             :first-name "MARGARET"}
                  #:customer{:email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :last-name "TAYLOR"
                             :first-name "DOROTHY"}])))

  (testing "Query data from all columns"
    (let [customer (-> *env*
                       :customer)
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:active 1
                             :activebool true
                             :address-id 5
                             :create-date date?
                             :customer-id 1
                             :email "MARY.SMITH@sakilacustomer.org"
                             :first-name "MARY"
                             :last-name "SMITH"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 6
                             :create-date date?
                             :customer-id 2
                             :email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :first-name "PATRICIA"
                             :last-name "JOHNSON"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 7
                             :create-date date?
                             :customer-id 3
                             :email "LINDA.WILLIAMS@sakilacustomer.org"
                             :first-name "LINDA"
                             :last-name "WILLIAMS"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 8
                             :create-date date?
                             :customer-id 4
                             :email "BARBARA.JONES@sakilacustomer.org"
                             :first-name "BARBARA"
                             :last-name "JONES"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 9
                             :create-date date?
                             :customer-id 5
                             :email "ELIZABETH.BROWN@sakilacustomer.org"
                             :first-name "ELIZABETH"
                             :last-name "BROWN"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 10
                             :create-date date?
                             :customer-id 6
                             :email "JENNIFER.DAVIS@sakilacustomer.org"
                             :first-name "JENNIFER"
                             :last-name "DAVIS"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 11
                             :create-date date?
                             :customer-id 7
                             :email "MARIA.MILLER@sakilacustomer.org"
                             :first-name "MARIA"
                             :last-name "MILLER"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 12
                             :create-date date?
                             :customer-id 8
                             :email "SUSAN.WILSON@sakilacustomer.org"
                             :first-name "SUSAN"
                             :last-name "WILSON"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 13
                             :create-date date?
                             :customer-id 9
                             :email "MARGARET.MOORE@sakilacustomer.org"
                             :first-name "MARGARET"
                             :last-name "MOORE"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 14
                             :create-date date?
                             :customer-id 10
                             :email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :first-name "DOROTHY"
                             :last-name "TAYLOR"
                             :last-update datetime?
                             :store-id 1}])))

  (testing "Expressions example"
    (let [customer (-> *env*
                       :customer
                       (r/extend :full-name [:concat :first-name " " :last-name])
                       (r/select [:full-name :email]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]

      (fact
       res =in=> [#:customer{:email "MARY.SMITH@sakilacustomer.org"
                             :full-name "MARY SMITH"}
                  #:customer{:email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :full-name "PATRICIA JOHNSON"}
                  #:customer{:email "LINDA.WILLIAMS@sakilacustomer.org"
                             :full-name "LINDA WILLIAMS"}
                  #:customer{:email "BARBARA.JONES@sakilacustomer.org"
                             :full-name "BARBARA JONES"}
                  #:customer{:email "ELIZABETH.BROWN@sakilacustomer.org"
                             :full-name "ELIZABETH BROWN"}
                  #:customer{:email "JENNIFER.DAVIS@sakilacustomer.org"
                             :full-name "JENNIFER DAVIS"}
                  #:customer{:email "MARIA.MILLER@sakilacustomer.org"
                             :full-name "MARIA MILLER"}
                  #:customer{:email "SUSAN.WILSON@sakilacustomer.org"
                             :full-name "SUSAN WILSON"}
                  #:customer{:email "MARGARET.MOORE@sakilacustomer.org"
                             :full-name "MARGARET MOORE"}
                  #:customer{:email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :full-name "DOROTHY TAYLOR"}]))))

(deftest column-alias
  ;; https://www.postgresqltutorial.com/postgresql-column-alias/
  (testing "Assigning a column alias"
    (let [customer (-> *env*
                       :customer
                       (r/rename :last-name :surname)
                       (r/select [:surname]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:surname "SMITH"}
                  #:customer{:surname "JOHNSON"}
                  #:customer{:surname "WILLIAMS"}
                  #:customer{:surname "JONES"}
                  #:customer{:surname "BROWN"}
                  #:customer{:surname "DAVIS"}
                  #:customer{:surname "MILLER"}
                  #:customer{:surname "WILSON"}
                  #:customer{:surname "MOORE"}
                  #:customer{:surname "TAYLOR"}]))))

(deftest order-by
  ;; https://www.postgresqltutorial.com/postgresql-order-by/
  (testing "Order by one column ASC"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :asc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by one column omitting ASC default"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [:first-name]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by one column DESC"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "HITE"
                             :first-name "ZACHARY"}
                  #:customer{:last-name "WATKINS"
                             :first-name "YVONNE"}
                  #:customer{:last-name "WEAVER"
                             :first-name "YOLANDA"}
                  #:customer{:last-name "RICHARDS"
                             :first-name "WILMA"}
                  #:customer{:last-name "MARKHAM"
                             :first-name "WILLIE"}
                  #:customer{:last-name "HOWELL"
                             :first-name "WILLIE"}
                  #:customer{:last-name "SATTERFIELD"
                             :first-name "WILLIAM"}
                  #:customer{:last-name "LUMPKIN"
                             :first-name "WILLARD"}
                  #:customer{:last-name "BULL"
                             :first-name "WESLEY"}
                  #:customer{:last-name "HARRISON"
                             :first-name "WENDY"}])))

  (testing "Order by multiple columns"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :asc] [:last-name :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by expression"
    (let [customer (-> *env*
                       :customer
                       (r/extend :len [:length :first-name])
                       (r/select [:first-name :len])
                       (r/order-by [[:len :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:first-name "CHRISTOPHER"
                             :len 11}
                  #:customer{:first-name "JACQUELINE"
                             :len 10}
                  #:customer{:first-name "FREDERICK"
                             :len 9}
                  #:customer{:first-name "CASSANDRA"
                             :len 9}
                  #:customer{:first-name "CHRISTINE"
                             :len 9}
                  #:customer{:first-name "CHRISTINA"
                             :len 9}
                  #:customer{:first-name "JOSEPHINE"
                             :len 9}
                  #:customer{:first-name "GWENDOLYN"
                             :len 9}
                  #:customer{:first-name "CATHERINE"
                             :len 9}
                  #:customer{:first-name "CONSTANCE"
                             :len 9}])))

  (testing "Order by and null"
    (let [sort-demo (-> *env*
                        :sort-demo
                        (r/select [:num])
                        (r/order-by [:num]))
          ;; We pass {:keep-nil? true} as the decomposition schema
          ;; override because otherwise Penkala will remove rows
          ;; where all primary key values are nil
          res (select! *env* sort-demo {} {:keep-nil? true})]
      (fact
       res =in=> [#:sort-demo{:num 1}
                  #:sort-demo{:num 2}
                  #:sort-demo{:num 3}
                  #:sort-demo{:num nil}]))

    (testing "Order by and NULLS FIRST"
      (let [sort-demo (-> *env*
                          :sort-demo
                          (r/select [:num])
                          (r/order-by [[:num :asc :nulls-first]]))
            ;; We pass {:keep-nil? true} as the decomposition schema
            ;; override because otherwise Penkala will remove rows
            ;; where all primary key values are nil
            res (select! *env* sort-demo {} {:keep-nil? true})]
        (fact
         res =in=> [#:sort-demo{:num nil}
                    #:sort-demo{:num 1}
                    #:sort-demo{:num 2}
                    #:sort-demo{:num 3}])))

    (testing "Order by and NULLS LAST"
      (let [sort-demo (-> *env*
                          :sort-demo
                          (r/select [:num])
                          (r/order-by [[:num :asc :nulls-last]]))
            ;; We pass {:keep-nil? true} as the decomposition schema
            ;; override because otherwise Penkala will remove rows
            ;; where all primary key values are nil
            res (select! *env* sort-demo {} {:keep-nil? true})]
        (fact
         res =in=> [#:sort-demo{:num 1}
                    #:sort-demo{:num 2}
                    #:sort-demo{:num 3}
                    #:sort-demo{:num nil}])))))

(deftest sql-distinct
  ;; https://www.postgresqltutorial.com/postgresql-select-distinct/
  (testing "Distinct one column"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct)
                            (r/select [:bcolor])
                            (r/order-by [:bcolor])) ;; add order-by to ensure test consistency
          res (select! *env* distinct-demo {} {:keep-nil? true})]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue"}
                  #:distinct-demo{:bcolor "green"}
                  #:distinct-demo{:bcolor "red"}
                  #:distinct-demo{:bcolor nil}])))

  (testing "Distinct multiple columns"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct)
                            (r/select [:bcolor :fcolor])
                            (r/order-by [:bcolor :fcolor])) ;; add order-by to ensure test consistency
          res (select! *env* distinct-demo)]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue", :fcolor "blue"}
                  #:distinct-demo{:bcolor "blue", :fcolor "green"}
                  #:distinct-demo{:bcolor "blue", :fcolor "red"}
                  #:distinct-demo{:bcolor "green", :fcolor "blue"}
                  #:distinct-demo{:bcolor "green", :fcolor "green"}
                  #:distinct-demo{:bcolor "green", :fcolor "red"}
                  #:distinct-demo{:bcolor "red", :fcolor "blue"}
                  #:distinct-demo{:bcolor "red", :fcolor "green"}
                  #:distinct-demo{:bcolor "red", :fcolor "red"}
                  #:distinct-demo{:bcolor "red", :fcolor nil}
                  #:distinct-demo{:bcolor nil, :fcolor "red"}])))

  (testing "Distinct on"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct [:bcolor])
                            (r/select [:bcolor :fcolor]))
          res (select! *env* distinct-demo)]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor "green"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor "red"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor nil
                                  :fcolor "red"}]))))

(deftest where
  ;; https://www.postgresqltutorial.com/postgresql-where/
  (testing "WHERE clause with the equal (=) operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :first-name "JAMIE"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}
                  #:customer{:last-name "WAUGH"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the AND operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:and
                                 [:= :first-name "JAMIE"]
                                 [:= :last-name "RICE"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the AND operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :first-name "JAMIE"])
                       (r/where [:= :last-name "RICE"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the OR operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:or
                                 [:= :last-name "RODRIGUEZ"]
                                 [:= :first-name "ADAM"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RODRIGUEZ"
                             :first-name "LAURA"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}])))

  (testing "WHERE clause with the OR operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :last-name "RODRIGUEZ"])
                       (r/or-where [:= :first-name "ADAM"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RODRIGUEZ"
                             :first-name "LAURA"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}])))

  (testing "WHERE clause with the IN operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:in :first-name ["ANN" "ANNE" "ANNIE"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "EVANS"
                             :first-name "ANN"}
                  #:customer{:last-name "POWELL"
                             :first-name "ANNE"}
                  #:customer{:last-name "RUSSELL"
                             :first-name "ANNIE"}])))

  (testing "WHERE clause with the LIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "ANN%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "HILL"
                             :first-name "ANNA"}
                  #:customer{:last-name "EVANS"
                             :first-name "ANN"}
                  #:customer{:last-name "POWELL"
                             :first-name "ANNE"}
                  #:customer{:last-name "RUSSELL"
                             :first-name "ANNIE"}
                  #:customer{:last-name "OLSON"
                             :first-name "ANNETTE"}])))

  (testing "WHERE clause with the BETWEEN operator"
    (let [customer (-> *env*
                       :customer
                       (r/extend :name-length [:length :first-name])
                       (r/select [:first-name :name-length])
                       (r/where [:and
                                 [:like :first-name "A%"]
                                 [:between :name-length 3 5]])
                       (r/limit 5))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:first-name "AMY"
                             :name-length 3}
                  #:customer{:first-name "ANNA"
                             :name-length 4}
                  #:customer{:first-name "ANN"
                             :name-length 3}
                  #:customer{:first-name "ALICE"
                             :name-length 5}
                  #:customer{:first-name "ANNE"
                             :name-length 4}])))

  (testing "WHERE clause with the NOT EQUAL operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:and
                                 [:like :first-name "BRA%"]
                                 [:<> :last-name "MOTLEY"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "GRAVES"
                             :first-name "BRANDY"}
                  #:customer{:last-name "HUEY"
                             :first-name "BRANDON"}
                  #:customer{:last-name "MCCURDY"
                             :first-name "BRAD"}]))))

(deftest limit
  ;; https://www.postgresqltutorial.com/postgresql-limit/
  (testing "Using LIMIT to constrain the number of returned rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/limit 5))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :release-year 2006}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :release-year 2006}
                  #:film{:title "ADAPTATION HOLES"
                         :film-id 3
                         :release-year 2006}
                  #:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}])))

  (testing "Using LIMIT with OFFSET"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/limit 4)
                   (r/offset 3))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}
                  #:film{:title "AGENT TRUMAN"
                         :film-id 6
                         :release-year 2006}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :release-year 2006}])))

  (testing "Using LIMIT / OFFSET to get top / bottom N rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :rental-rate])
                   (r/order-by [[:rental-rate :desc]])
                   (r/limit 10))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AMERICAN CIRCUS"
                         :film-id 21
                         :rental-rate 4.99M}
                  #:film{:title "APACHE DIVINE"
                         :film-id 31
                         :rental-rate 4.99M}
                  #:film{:title "ALI FOREVER"
                         :film-id 13
                         :rental-rate 4.99M}
                  #:film{:title "AMELIE HELLFIGHTERS"
                         :film-id 20
                         :rental-rate 4.99M}
                  #:film{:title "ANTHEM LUKE"
                         :film-id 28
                         :rental-rate 4.99M}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :rental-rate 4.99M}
                  #:film{:title "AIRPORT POLLOCK"
                         :film-id 8
                         :rental-rate 4.99M}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :rental-rate 4.99M}
                  #:film{:title "ALADDIN CALENDAR"
                         :film-id 10
                         :rental-rate 4.99M}
                  #:film{:title "APOCALYPSE FLAMINGOS"
                         :film-id 32
                         :rental-rate 4.99M}]))))

(deftest fetch
  ;; https://www.postgresqltutorial.com/postgresql-fetch/
  (testing "Using FETCH to constrain the number of returned rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/fetch 5))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :release-year 2006}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :release-year 2006}
                  #:film{:title "ADAPTATION HOLES"
                         :film-id 3
                         :release-year 2006}
                  #:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}])))

  (testing "Using FETCH with OFFSET"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/fetch 4)
                   (r/offset 3))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}
                  #:film{:title "AGENT TRUMAN"
                         :film-id 6
                         :release-year 2006}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :release-year 2006}])))

  (testing "Using FETCH / OFFSET to get top / bottom N rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :rental-rate])
                   (r/order-by [[:rental-rate :desc]])
                   (r/fetch 10))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AMERICAN CIRCUS"
                         :film-id 21
                         :rental-rate 4.99M}
                  #:film{:title "APACHE DIVINE"
                         :film-id 31
                         :rental-rate 4.99M}
                  #:film{:title "ALI FOREVER"
                         :film-id 13
                         :rental-rate 4.99M}
                  #:film{:title "AMELIE HELLFIGHTERS"
                         :film-id 20
                         :rental-rate 4.99M}
                  #:film{:title "ANTHEM LUKE"
                         :film-id 28
                         :rental-rate 4.99M}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :rental-rate 4.99M}
                  #:film{:title "AIRPORT POLLOCK"
                         :film-id 8
                         :rental-rate 4.99M}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :rental-rate 4.99M}
                  #:film{:title "ALADDIN CALENDAR"
                         :film-id 10
                         :rental-rate 4.99M}
                  #:film{:title "APOCALYPSE FLAMINGOS"
                         :film-id 32
                         :rental-rate 4.99M}]))))

(deftest in
  ;; https://www.postgresqltutorial.com/postgresql-in/
  (testing "IN Operator"
    (let [rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:in :customer-id [1 2]]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 76}
                  #:rental{:customer-id 2
                           :return-date datetime?
                           :rental-id 320}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 573}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 1185}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 1422}]))

    (testing "Emulating IN with OR"
      (let [rental (-> *env*
                       :rental
                       (r/select [:customer-id :rental-id :return-date])
                       (r/where [:or
                                 [:= :customer-id 1]
                                 [:= :customer-id 2]]))
            res (-> (select! *env* rental)
                    (subvec 0 5))]
        (fact
         res =in=> [#:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 76}
                    #:rental{:customer-id 2
                             :return-date datetime?
                             :rental-id 320}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 573}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 1185}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 1422}]))))

  (testing "NOT IN Operator"
    (let [rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:not-in :customer-id [1 2]]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 459
                           :return-date datetime?
                           :rental-id 2}
                  #:rental{:customer-id 408
                           :return-date datetime?
                           :rental-id 3}
                  #:rental{:customer-id 333
                           :return-date datetime?
                           :rental-id 4}
                  #:rental{:customer-id 222
                           :return-date datetime?
                           :rental-id 5}
                  #:rental{:customer-id 549
                           :return-date datetime?
                           :rental-id 6}]))

    (testing "Emulating NOT IN with AND"
      (let [rental (-> *env*
                       :rental
                       (r/select [:customer-id :rental-id :return-date])
                       (r/where [:and
                                 [:<> :customer-id 1]
                                 [:<> :customer-id 2]]))
            res (-> (select! *env* rental)
                    (subvec 0 5))]
        (fact
         res =in=> [#:rental{:customer-id 459
                             :return-date datetime?
                             :rental-id 2}
                    #:rental{:customer-id 408
                             :return-date datetime?
                             :rental-id 3}
                    #:rental{:customer-id 333
                             :return-date datetime?
                             :rental-id 4}
                    #:rental{:customer-id 222
                             :return-date datetime?
                             :rental-id 5}
                    #:rental{:customer-id 549
                             :return-date datetime?
                             :rental-id 6}]))))

  (testing "IN with a subquery"
    (let [rental-customers (-> *env*
                               :rental
                               (r/select [:customer-id])
                               (r/where [:=
                                         [:cast "2005-05-27" "date"]
                                         [:cast :return-date "date"]]))
          rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:in :customer-id rental-customers]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 549
                           :return-date datetime?
                           :rental-id 6}
                  #:rental{:customer-id 269
                           :return-date datetime?
                           :rental-id 7}
                  #:rental{:customer-id 575
                           :return-date datetime?
                           :rental-id 17}
                  #:rental{:customer-id 185
                           :return-date datetime?
                           :rental-id 20}
                  #:rental{:customer-id 350
                           :return-date datetime?
                           :rental-id 24}]))))

(deftest between
  ;; https://www.postgresqltutorial.com/postgresql-between/

  (testing "BETWEEN operator"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :amount])
                      (r/where [:between :amount 8 9]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 271
                            :amount 8.99M
                            :payment-id 16058}
                  #:payment{:customer-id 293
                            :amount 8.99M
                            :payment-id 16102}
                  #:payment{:customer-id 299
                            :amount 8.99M
                            :payment-id 16113}
                  #:payment{:customer-id 315
                            :amount 8.99M
                            :payment-id 16154}
                  #:payment{:customer-id 316
                            :amount 8.99M
                            :payment-id 16157}])))

  (testing "NOT BETWEEN operator"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :amount])
                      (r/where [:not [:between :amount 8 9]]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 269
                            :amount 1.99M
                            :payment-id 16050}
                  #:payment{:customer-id 269
                            :amount 0.99M
                            :payment-id 16051}
                  #:payment{:customer-id 269
                            :amount 6.99M
                            :payment-id 16052}
                  #:payment{:customer-id 269
                            :amount 0.99M
                            :payment-id 16053}
                  #:payment{:customer-id 269
                            :amount 4.99M
                            :payment-id 16054}])))

  (testing "BETWEEN dates"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :payment-date :amount])
                      (r/where [:between :payment-date [:cast "2020-02-07" "date"] [:cast "2020-02-15" "date"]]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 284
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17270}
                  #:payment{:customer-id 285
                            :amount 7.99M
                            :payment-date datetime?
                            :payment-id 17273}
                  #:payment{:customer-id 306
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17356}
                  #:payment{:customer-id 310
                            :amount 4.99M
                            :payment-date datetime?
                            :payment-id 17373}
                  #:payment{:customer-id 323
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17424}]))))

(deftest like
  ;; https://www.postgresqltutorial.com/postgresql-like/

  (testing "LIKE operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "JEN%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:last-name "TERRY"
                             :first-name "JENNIE"}
                  #:customer{:last-name "CASTRO"
                             :first-name "JENNY"}])))

  (testing "LIKE operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "%ER%"]))
          res (-> (select! *env* customer)
                  (subvec 0 5))]
      (fact
       res =in=> [#:customer{:last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:last-name "LEE"
                             :first-name "KIMBERLY"}
                  #:customer{:last-name "CAMPBELL"
                             :first-name "CATHERINE"}
                  #:customer{:last-name "MORRIS"
                             :first-name "HEATHER"}
                  #:customer{:last-name "ROGERS"
                             :first-name "TERESA"}])))

  (testing "LIKE operator (3)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "_HER%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "MURPHY"
                             :first-name "CHERYL"}
                  #:customer{:last-name "WATSON"
                             :first-name "THERESA"}
                  #:customer{:last-name "MARSHALL"
                             :first-name "SHERRY"}
                  #:customer{:last-name "RHODES"
                             :first-name "SHERRI"}])))

  (testing "NOT LIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:not-like :first-name "JEN%"]))
          res (-> (select! *env* customer)
                  (subvec 0 5))]
      (fact
       res =in=> [#:customer{:last-name "SMITH"
                             :first-name "MARY"}
                  #:customer{:last-name "JOHNSON"
                             :first-name "PATRICIA"}
                  #:customer{:last-name "WILLIAMS"
                             :first-name "LINDA"}
                  #:customer{:last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:last-name "BROWN"
                             :first-name "ELIZABETH"}])))

  (testing "ILIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:ilike :first-name "bar%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:last-name "LOVELACE"
                             :first-name "BARRY"}]))))

(deftest is-null
  ;; https://www.postgresqltutorial.com/postgresql-is-null/

  (testing "IS NULL operator"
    (let [contacts (-> *env*
                       :contacts
                       (r/select [:id :first-name :last-name :email :phone])
                       (r/where [:is-null :phone]))
          res (select! *env* contacts)]
      (fact
       res =in=> [#:contacts{:email "john.doe@example.com"
                             :last-name "Doe"
                             :phone nil
                             :first-name "John"
                             :id 1}])))

  (testing "IS NOT NULL operator"
    (let [contacts (-> *env*
                       :contacts
                       (r/select [:id :first-name :last-name :email :phone])
                       (r/where [:is-not-null :phone]))
          res (select! *env* contacts)]
      (fact
       res =in=> [#:contacts{:email "lily.bush@example.com"
                             :last-name "Bush"
                             :phone "(408-234-2764)"
                             :first-name "Lily"
                             :id 2}]))))

(deftest inner-join
  ;; https://www.postgresqltutorial.com/postgresql-inner-join/

  (testing "INNER JOIN two tables"
    (let [payment (-> *env*
                      :payment
                      (r/select [:amount :payment-date :customer-id]))
          customer (-> *env*
                       :customer
                       (r/select [:customer-id :first-name :last-name])
                       (r/inner-join payment :payments [:= :customer-id :payments/customer-id])
                       (r/order-by [:payments/payment-date]))
          res (-> (select! *env* customer)
                  (subvec 0 2))]
      (fact
       res =in=> [#:customer{:customer-id 130
                             :last-name "HUNTER"
                             :first-name "CHARLOTTE"
                             :payments [#:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}]}
                  #:customer{:customer-id 459
                             :last-name "COLLAZO"
                             :first-name "TOMMY"
                             :payments [#:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}]}]))
    (testing "LEFT LATERAL JOIN two tables"
      ;; This example expands on the previous one (not present on the https://www.postgresqltutorial.com/postgresql-inner-join page)

      ;; Instead of getting all customers with all payments, we'll get first two customers each with first two
      ;; payments. This will require a bit more work, but is a good example of using more advanced composability
      ;; features of Penkala.

      (let [customer (:customer *env*)
            payment (:payment *env*)
            ;; First we want to get the customer ids for customer which have payment records. We want to get 
            ;; the first two customer ids from payments ordered by payment date (so customer-ids that made the)
            ;; first payments. Since we need two *different* ids (a customer could make multiple payments in a row)
            ;; we use the window function to partition payments by customer-ids and then get rows where row-number is 1.
            ;; `(r/wrap relation)` function used here forces another subselect layer so we can select only the
            ;; `:customer-id` column from the outer layer. This allows us to pass this subselect to the IN operation
            ;; in the final query.
            customers-with-payments (-> payment
                                        (r/extend-with-window :row-number [:row-number] [:customer-id] [:payment-date])
                                        (r/select [:customer-id :row-number])
                                        (r/order-by [:payment-date])
                                        (r/wrap)
                                        (r/where [:= :row-number 1])
                                        (r/select [:customer-id])
                                        (r/limit 2))

            ;; This relation will be laft lateral joined to the parent relation, so we'll get
            ;; two payments per customer. Notice `(r/with-parent relation customer)` and `[:parent-scope :customer-id]`
            ;; - this allows the inner subquery to access the columns from the parent select without having
            ;; to care about the aliasing performed by Penkala. For each customer we'll get two payments
            payments-for-customer (-> payment
                                      (r/select [:amount :payment-date :customer-id])
                                      (r/with-parent customer)
                                      (r/order-by [:payment-date])
                                      (r/where [:= :customer-id [:parent-scope :customer-id]])
                                      (r/limit 2))

            ;; Final relation - we select customers where ids are in the result of the `customers-with-payments` query
            ;; and we left lateral join `payments-for-customer` which will select two payments for each customer
            customer (-> customer
                         (r/left-lateral-join payments-for-customer :payments true)
                         (r/where [:in :customer-id customers-with-payments])
                         (r/select [:customer-id :first-name :last-name])
                         (r/order-by [:payments/payment-date]))

            res  (select! *env* customer)]
        (fact
         res =in=> [#:customer{:customer-id 130
                               :last-name "HUNTER"
                               :first-name "CHARLOTTE"
                               :payments [#:payment{:customer-id 130
                                                    :amount 2.99M
                                                    :payment-date datetime?}
                                          #:payment{:customer-id 130
                                                    :amount 2.99M
                                                    :payment-date datetime?}]}
                    #:customer{:customer-id 459
                               :last-name "COLLAZO"
                               :first-name "TOMMY"
                               :payments [#:payment{:customer-id 459
                                                    :amount 2.99M
                                                    :payment-date datetime?}
                                          #:payment{:customer-id 459
                                                    :amount 0.99M
                                                    :payment-date datetime?}]}]))))

  (testing "INNER JOIN three tables"
    (let [staff (-> *env*
                    :staff
                    (r/select [:first-name :last-name :staff-id]))

          payment (-> *env*
                      :payment
                      (r/select [:amount :payment-date :customer-id])
                      (r/inner-join staff :staff [:= :staff-id :staff/staff-id]))

          customer (-> *env*
                       :customer
                       (r/select [:customer-id :first-name :last-name])
                       (r/inner-join payment :payments [:= :customer-id :payments/customer-id])
                       (r/order-by [:payments/payment-date]))
          res (-> (select! *env* customer)
                  (subvec 0 2))]
      (fact
       res =in=> [#:customer{:customer-id 130
                             :last-name "HUNTER"
                             :first-name "CHARLOTTE"
                             :payments [#:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}]}
                  #:customer{:customer-id 459
                             :last-name "COLLAZO"
                             :first-name "TOMMY"
                             :payments [#:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}]}]))))

(deftest left-join
  ;; https://www.postgresqltutorial.com/postgresql-left-join/

  (testing "LEFT JOIN (1)"
    (let [inventory (-> *env*
                        :inventory
                        (r/select [:film-id :inventory-id]))
          film (-> *env*
                   :film
                   ;; Last argument is join projection - `inventory` relation exposes two columns - `film-id` and
                   ;; `inventory-id` but we're selecting only `inventory-id` from the joined relation
                   (r/left-join inventory :inventory [:= :film-id :inventory/film-id] [:inventory/inventory-id])
                   (r/order-by [:title :inventory/inventory-id])
                   (r/select [:film-id :title]))
          res (-> (select! *env* film)
                  (subvec 0 2))]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :inventory [#:inventory{:inventory-id 1}
                                     #:inventory{:inventory-id 2}
                                     #:inventory{:inventory-id 3}
                                     #:inventory{:inventory-id 4}
                                     #:inventory{:inventory-id 5}
                                     #:inventory{:inventory-id 6}
                                     #:inventory{:inventory-id 7}
                                     #:inventory{:inventory-id 8}]}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :inventory [#:inventory{:inventory-id 9}
                                     #:inventory{:inventory-id 10}
                                     #:inventory{:inventory-id 11}]}])))

  (testing "LEFT JOIN (2)"
    (let [inventory (-> *env*
                        :inventory
                        (r/select [:film-id :inventory-id]))
          film (-> *env*
                   :film
                   (r/left-join inventory :inventory [:= :film-id :inventory/film-id] [:inventory/inventory-id])
                   (r/where [:is-null :inventory/film-id])
                   (r/select [:film-id :title])
                   (r/order-by [:title]))
          res (-> (select! *env* film)
                  (subvec 0 2))]
      (fact
       res =in=> [#:film{:title "ALICE FANTASIA"
                         :film-id 14
                         :inventory []}
                  #:film{:title "APOLLO TEEN"
                         :film-id 33
                         :inventory []}]))))

(deftest right-join
  ;; https://www.postgresqltutorial.com/postgresql-right-join/
  (testing "RIGHT JOIN (1)"
    (let [film-reviews (-> *env*
                           :film-reviews
                           (r/select [:film-id :review]))
          films (-> *env*
                    :films
                    (r/select [:title])
                    (r/right-join film-reviews :film-reviews [:using :film-id] [:film-reviews/review]))
        ;; Last argument is the decomposition schema. Right joins can't be decomposed correctly if the "left"
        ;; table is missing values, so we're passing false to get unprocessed list of results
          res (select! *env* films nil false)]
      (fact
       res =in=> [{:title "Joker", :film-reviews/review "Excellent"}
                  {:title "Joker", :film-reviews/review "Awesome"}
                  {:title "Avengers: Endgame", :film-reviews/review "Cool"}
                  {:title nil, :film-reviews/review "Beautiful"}])))
  (testing "RIGHT JOIN (2)"
    (let [film-reviews (-> *env*
                           :film-reviews
                           (r/select [:film-id :review]))
          films (-> *env*
                    :films
                    (r/select [:title])
                    (r/where [:is-null :title])
                    (r/right-join film-reviews :film-reviews [:using :film-id] [:film-reviews/review]))
        ;; Last argument is the decomposition schema. Right joins can't be decomposed correctly if the "left"
        ;; table is missing values, so we're passing false to get unprocessed list of results
          res (select! *env* films nil false)]
      (fact
       res =in=> [{:title nil, :film-reviews/review "Beautiful"}]))))

(deftest self-join
  ;; https://www.postgresqltutorial.com/postgresql-self-join/
  (testing "1 - Querying hierarchical data example"
    (let [manager (-> *env*
                      :employee
                      (r/extend :manager [:concat :first-name " " :last-name]))
          employee (-> *env*
                       :employee
                       (r/extend :employee [:concat :first-name " " :last-name]))
          hierarchy (-> employee
                        (r/left-join manager :manager [:= :manager-id :manager/employee-id] [:manager/manager])
                        (r/select [:employee])
                        (r/order-by [:manager/manager]))
        ;; Last argument is the decomposition schema. We're passing false to get a flat list of results
          res (select! *env* hierarchy nil false)]
      (fact
       res =in=> [{:employee "Windy Hays", :manager/manager nil}
                  {:employee "Sau Norman", :manager/manager "Ava Christensen"}
                  {:employee "Anna Reeves", :manager/manager "Ava Christensen"}
                  {:employee "Salley Lester", :manager/manager "Hassan Conner"}
                  {:employee "Kelsie Hays", :manager/manager "Hassan Conner"}
                  {:employee "Tory Goff", :manager/manager "Hassan Conner"}
                  {:employee "Ava Christensen", :manager/manager "Windy Hays"}
                  {:employee "Hassan Conner", :manager/manager "Windy Hays"}])))

  (testing "2 - Comparing the rows with the same table"
    (let [film (:film *env*)
          films (-> film
                    (r/inner-join film :film-2 [:and [:<> :film-id :film-2/film-id] [:= :length :film-2/length]] [:film-2/title])
                    (r/select [:title :length])
                    (r/order-by [:title]))
        ;; Last argument is the decomposition schema. We're passing false to get a flat list of results
          res (-> (select! *env* films nil false)
                  (subvec 0 10))]
      (fact
       res =in=> [{:title "ACADEMY DINOSAUR", :length 86, :film-2/title "YENTL IDAHO"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "ANNIE IDENTITY"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "MIDNIGHT WESTWARD"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "GANDHI KWAI"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "RUSH GOODFELLAS"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "PELICAN COMFORTS"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "PARADISE SABRINA"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "ODDS BOOGIE"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "MIDSUMMER GROUNDHOG"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "HEAVEN FREEDOM"}]))))

(deftest full-outer-join
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-full-outer-join/
  (testing "Full outer join"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/full-join departments :departments [:= :department-id :departments/department-id]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id 1, :employee-name "Bette Nicholson", :employee-id 1, :departments/department-id 1, :departments/department-name "Sales"}
                  {:department-id 1, :employee-name "Christian Gable", :employee-id 2, :departments/department-id 1, :departments/department-name "Sales"}
                  {:department-id 2, :employee-name "Joe Swank", :employee-id 3, :departments/department-id 2, :departments/department-name "Marketing"}
                  {:department-id 3, :employee-name "Fred Costner", :employee-id 4, :departments/department-id 3, :departments/department-name "HR"}
                  {:department-id 4, :employee-name "Sandra Kilmer", :employee-id 5, :departments/department-id 4, :departments/department-name "IT"}
                  {:department-id nil, :employee-name "Julia Mcqueen", :employee-id 6, :departments/department-id nil, :departments/department-name nil}
                  {:department-id nil, :employee-name nil, :employee-id nil, :departments/department-id 5, :departments/department-name "Production"}])))
  (testing "Full outer join - find departments without employees"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/full-join departments :departments [:= :department-id :departments/department-id])
                        (r/where [:is-null :employee-name]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id nil, :employee-name nil, :employee-id nil, :departments/department-id 5, :departments/department-name "Production"}])))
  (testing "Full outer join - find employees without department"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/full-join departments :departments [:= :department-id :departments/department-id])
                        (r/where [:is-null :departments/department-name]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id nil, :employee-name "Julia Mcqueen", :employee-id 6, :departments/department-id nil, :departments/department-name nil}]))))

(deftest cross-join
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-cross-join/
  (let [t-2 (:t-2 *env*)
        t-1 (-> *env*
                :t-1
                (r/cross-join t-2 :t-2))
        res (select! *env* t-1 nil false)]
    (fact
     res =in=> [{:label "A", :t-2/score 1}
                {:label "B", :t-2/score 1}
                {:label "A", :t-2/score 2}
                {:label "B", :t-2/score 2}
                {:label "A", :t-2/score 3}
                {:label "B", :t-2/score 3}])))

;; Penkala doesn't support NATURAL JOIN so these tests are skipped - https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-natural-join/

(deftest group-by'
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-group-by/
  (testing "Using PostgreSQL GROUP BY without an aggregate function example"
    (let [payments (-> *env*
                       :payment
                       (r/select [:customer-id])
                       (r/group-by [:customer-id]))
          res (-> (select! *env* payments nil false)
                  (subvec 0 10))]
      (fact
       res =in=> [{:customer-id 184}
                  {:customer-id 87}
                  {:customer-id 477}
                  {:customer-id 273}
                  {:customer-id 550}
                  {:customer-id 51}
                  {:customer-id 394}
                  {:customer-id 272}
                  {:customer-id 70}
                  {:customer-id 190}])))

  (testing "Using PostgreSQL GROUP BY with SUM() function example"
    (let [payments-explicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/select [:customer-id :sum-amount])
                                         (r/group-by [:customer-id]))
          ;; When using extend-with-aggregate Penkala can infer GROUP BY clause
          payments-implicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/select [:customer-id :sum-amount]))
          res-explicit (-> (select! *env* payments-explicit-group-by nil false)
                           (subvec 0 10))
          res-implicit (-> (select! *env* payments-implicit-group-by nil false)
                           (subvec 0 10))]

      (facts
       res-implicit => res-explicit
       res-implicit =in=> [{:customer-id 184, :sum-amount 90.77M}
                           {:customer-id 87, :sum-amount 145.70M}
                           {:customer-id 477, :sum-amount 109.78M}
                           {:customer-id 273, :sum-amount 157.65M}
                           {:customer-id 550, :sum-amount 159.68M}
                           {:customer-id 51, :sum-amount 138.67M}
                           {:customer-id 394, :sum-amount 84.78M}
                           {:customer-id 272, :sum-amount 98.80M}
                           {:customer-id 70, :sum-amount 80.82M}
                           {:customer-id 190, :sum-amount 110.73M}]
       res-explicit =in=> [{:customer-id 184, :sum-amount 90.77M}
                           {:customer-id 87, :sum-amount 145.70M}
                           {:customer-id 477, :sum-amount 109.78M}
                           {:customer-id 273, :sum-amount 157.65M}
                           {:customer-id 550, :sum-amount 159.68M}
                           {:customer-id 51, :sum-amount 138.67M}
                           {:customer-id 394, :sum-amount 84.78M}
                           {:customer-id 272, :sum-amount 98.80M}
                           {:customer-id 70, :sum-amount 80.82M}
                           {:customer-id 190, :sum-amount 110.73M}])))

  (testing "Using PostgreSQL GROUP BY clause with the JOIN clause"
    (let [customer (-> *env*
                       :customer
                       (r/extend :full-name [:concat :first-name " " :last-name]))
          payments-explicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/inner-join customer :customer [:using :customer-id] [:customer/full-name])
                                         (r/select [:sum-amount])
                                         (r/order-by [:sum-amount]) ;; Add order-by to get consistent results
                                         (r/group-by [:customer/full-name]))
          ;; When using extend-with-aggregate Penkala can infer GROUP BY clause
          payments-implicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/inner-join customer :customer [:using :customer-id] [:customer/full-name])
                                         (r/order-by [:sum-amount]) ;; Add order-by to get consistent results
                                         (r/select [:sum-amount]))
          res-explicit (-> (select! *env* payments-explicit-group-by nil false)
                           (subvec 0 10))
          res-implicit (-> (select! *env* payments-implicit-group-by nil false)
                           (subvec 0 10))]

      (facts
       res-implicit => res-explicit
       res-implicit =in=> [{:customer/full-name "CAROLINE BOWMAN", :sum-amount 50.85M}
                           {:customer/full-name "LEONA OBRIEN", :sum-amount 50.86M}
                           {:customer/full-name "BRIAN WYMAN", :sum-amount 52.88M}
                           {:customer/full-name "JOHNNY TURPIN", :sum-amount 57.81M}
                           {:customer/full-name "ANNIE RUSSELL", :sum-amount 58.82M}
                           {:customer/full-name "KATHERINE RIVERA", :sum-amount 58.86M}
                           {:customer/full-name "TIFFANY JORDAN", :sum-amount 59.86M}
                           {:customer/full-name "ANITA MORALES", :sum-amount 62.85M}
                           {:customer/full-name "MATTIE HOFFMAN", :sum-amount 64.78M}
                           {:customer/full-name "KIRK STCLAIR", :sum-amount 64.81M}]
       res-explicit =in=> [{:customer/full-name "CAROLINE BOWMAN", :sum-amount 50.85M}
                           {:customer/full-name "LEONA OBRIEN", :sum-amount 50.86M}
                           {:customer/full-name "BRIAN WYMAN", :sum-amount 52.88M}
                           {:customer/full-name "JOHNNY TURPIN", :sum-amount 57.81M}
                           {:customer/full-name "ANNIE RUSSELL", :sum-amount 58.82M}
                           {:customer/full-name "KATHERINE RIVERA", :sum-amount 58.86M}
                           {:customer/full-name "TIFFANY JORDAN", :sum-amount 59.86M}
                           {:customer/full-name "ANITA MORALES", :sum-amount 62.85M}
                           {:customer/full-name "MATTIE HOFFMAN", :sum-amount 64.78M}
                           {:customer/full-name "KIRK STCLAIR", :sum-amount 64.81M}])))

  (testing "Using PostgreSQL GROUP BY with COUNT() function example"
    (let [payments-explicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :count-payments [:count :payment-id])
                                         (r/select [:staff-id :count-payments])
                                         (r/group-by [:staff-id]))
          ;; When using extend-with-aggregate Penkala can infer GROUP BY clause
          payments-implicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :count-payments [:count :payment-id])
                                         (r/select [:staff-id :count-payments]))
          res-explicit (select! *env* payments-explicit-group-by nil false)
          res-implicit (select! *env* payments-implicit-group-by nil false)]

      (facts
       res-implicit => res-explicit
       res-implicit =in=> [{:count-payments 7992, :staff-id 2}
                           {:count-payments 8057, :staff-id 1}]
       res-explicit =in=> [{:count-payments 7992, :staff-id 2}
                           {:count-payments 8057, :staff-id 1}])))

  (testing "Using PostgreSQL GROUP BY with SUM() function example"
    (let [payments-explicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/select [:customer-id :sum-amount])
                                         (r/group-by [:customer-id]))
          ;; When using extend-with-aggregate Penkala can infer GROUP BY clause
          payments-implicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/select [:customer-id :sum-amount]))
          res-explicit (-> (select! *env* payments-explicit-group-by nil false)
                           (subvec 0 10))
          res-implicit (-> (select! *env* payments-implicit-group-by nil false)
                           (subvec 0 10))]

      (facts
       res-implicit => res-explicit
       res-implicit =in=> [{:customer-id 184, :sum-amount 90.77M}
                           {:customer-id 87, :sum-amount 145.70M}
                           {:customer-id 477, :sum-amount 109.78M}
                           {:customer-id 273, :sum-amount 157.65M}
                           {:customer-id 550, :sum-amount 159.68M}
                           {:customer-id 51, :sum-amount 138.67M}
                           {:customer-id 394, :sum-amount 84.78M}
                           {:customer-id 272, :sum-amount 98.80M}
                           {:customer-id 70, :sum-amount 80.82M}
                           {:customer-id 190, :sum-amount 110.73M}]
       res-explicit =in=> [{:customer-id 184, :sum-amount 90.77M}
                           {:customer-id 87, :sum-amount 145.70M}
                           {:customer-id 477, :sum-amount 109.78M}
                           {:customer-id 273, :sum-amount 157.65M}
                           {:customer-id 550, :sum-amount 159.68M}
                           {:customer-id 51, :sum-amount 138.67M}
                           {:customer-id 394, :sum-amount 84.78M}
                           {:customer-id 272, :sum-amount 98.80M}
                           {:customer-id 70, :sum-amount 80.82M}
                           {:customer-id 190, :sum-amount 110.73M}])))

  (testing "Using PostgreSQL GROUP BY clause with date column"
    (let [payments-explicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/extend :paid-date [:date :payment-date])
                                         (r/select [:paid-date :sum-amount])
                                         (r/group-by [:paid-date]))
          ;; When using extend-with-aggregate Penkala can infer GROUP BY clause
          payments-implicit-group-by (-> *env*
                                         :payment
                                         (r/extend-with-aggregate :sum-amount [:sum :amount])
                                         (r/extend :paid-date [:date :payment-date])
                                         (r/select [:paid-date :sum-amount]))
          res-explicit (-> (select! *env* payments-explicit-group-by nil false)
                           (subvec 0 10))
          res-implicit (-> (select! *env* payments-implicit-group-by nil false)
                           (subvec 0 10))]

      (facts
       res-implicit => res-explicit
       res-implicit =in=> [{:paid-date date?, :sum-amount 1135.32M}
                           {:paid-date date?, :sum-amount 2727.76M}
                           {:paid-date date?, :sum-amount 1485.49M}
                           {:paid-date date?, :sum-amount 632.41M}
                           {:paid-date date?, :sum-amount 62.86M}
                           {:paid-date date?, :sum-amount 2464.06M}
                           {:paid-date date?, :sum-amount 514.18M}
                           {:paid-date date?, :sum-amount 671.43M}
                           {:paid-date date?, :sum-amount 224.51M}
                           {:paid-date date?, :sum-amount 2742.54M}]
       res-explicit =in=> [{:paid-date date?, :sum-amount 1135.32M}
                           {:paid-date date?, :sum-amount 2727.76M}
                           {:paid-date date?, :sum-amount 1485.49M}
                           {:paid-date date?, :sum-amount 632.41M}
                           {:paid-date date?, :sum-amount 62.86M}
                           {:paid-date date?, :sum-amount 2464.06M}
                           {:paid-date date?, :sum-amount 514.18M}
                           {:paid-date date?, :sum-amount 671.43M}
                           {:paid-date date?, :sum-amount 224.51M}
                           {:paid-date date?, :sum-amount 2742.54M}]))))

(deftest union'
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-union/
  (testing "Simple PostgreSQL UNION example"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/union most-popular-films))
          res (select! *env* rel)]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "12 Angry Men", :release-year 1957}
                  #:top-rated-films-most-popular-films{:title "Greyhound", :release-year 2020}
                  #:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}
                  #:top-rated-films-most-popular-films{:title "An American Pickle", :release-year 2020}
                  #:top-rated-films-most-popular-films{:title "The Shawshank Redemption", :release-year 1994}])))

  (testing "PostgreSQL UNION ALL example"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/union-all most-popular-films))
          ;; We need to use :keep-duplicates? because otherwise Penkala decomposition layer will remove duplicate rows
          res (select! *env* rel nil {:keep-duplicates? true})]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "The Shawshank Redemption", :release-year 1994}
                  #:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}
                  #:top-rated-films-most-popular-films{:title "12 Angry Men", :release-year 1957}
                  #:top-rated-films-most-popular-films{:title "An American Pickle", :release-year 2020}
                  #:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}
                  #:top-rated-films-most-popular-films{:title "Greyhound", :release-year 2020}])))

  (testing "PostgreSQL UNION ALL with ORDER BY clause example"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/union-all most-popular-films)
                  (r/order-by [:title]))
          ;; We need to use :keep-duplicates? because otherwise Penkala decomposition layer will remove duplicate rows
          res (select! *env* rel nil {:keep-duplicates? true})]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "12 Angry Men", :release-year 1957}
                  #:top-rated-films-most-popular-films{:title "An American Pickle", :release-year 2020}
                  #:top-rated-films-most-popular-films{:title "Greyhound", :release-year 2020}
                  #:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}
                  #:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}
                  #:top-rated-films-most-popular-films{:title "The Shawshank Redemption", :release-year 1994}]))))

(deftest intersect'
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-intersect/
  (testing "PostgreSQL INTERSECT operator examples"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/intersect most-popular-films))
          res (select! *env* rel)]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "The Godfather", :release-year 1972}]))))

(deftest except'
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-except/
  (testing "PostgreSQL EXCEPT operator example"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/except most-popular-films))
          res (select! *env* rel)]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "12 Angry Men", :release-year 1957}
                  #:top-rated-films-most-popular-films{:title "The Shawshank Redemption", :release-year 1994}])))
  (testing "PostgreSQL EXCEPT with ORDER BY clause example"
    (let [{:keys [top-rated-films most-popular-films]} *env*
          rel (-> top-rated-films
                  (r/except most-popular-films)
                  (r/order-by [[:title :desc]]))
          res (select! *env* rel)]
      (fact
       res =in=> [#:top-rated-films-most-popular-films{:title "The Shawshank Redemption", :release-year 1994}
                  #:top-rated-films-most-popular-films{:title "12 Angry Men", :release-year 1957}]))))

(deftest having
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-having/
  (testing "Using PostgreSQL HAVING clause with SUM function example"
    (let [payments (-> *env*
                       :payment
                       (r/extend-with-aggregate :sum-amount [:sum :amount])
                       (r/select [:customer-id :sum-amount])
                       (r/having [:> :sum-amount 200]))
          res (select! *env* payments)]
      (fact
       res =in=> [#:payment{:customer-id 526, :sum-amount 221.55M}
                  #:payment{:customer-id 148, :sum-amount 216.54M}])))
  (testing "PostgreSQL HAVING clause with COUNT example"
    (let [stores (-> *env*
                     :customer
                     (r/extend-with-aggregate :customer-id-count [:count :customer-id])
                     (r/select [:store-id :customer-id-count])
                     (r/having [:> :customer-id-count 300]))
          res (select! *env* stores)]
      (fact
       res =in=> [#:customer{:store-id 1, :customer-id-count 326}]))))

(deftest grouping-sets
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-grouping-sets/
  (testing "Introduction to PostgreSQL GROUPING SETS"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [[:grouping-sets [:brand :segment] [:brand] [:segment] []]]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 700, :brand nil, :segment nil}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 300, :brand "ABC", :segment nil}
                  #:sales{:sum-quantity 400, :brand "XYZ", :segment nil}
                  #:sales{:sum-quantity 500, :brand nil, :segment "Basic"}
                  #:sales{:sum-quantity 200, :brand nil, :segment "Premium"}])))

  (testing "Grouping function 1"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/extend :grouping-brand [:grouping :brand])
                    (r/extend :grouping-segment [:grouping :segment])
                    (r/select [:grouping-brand :grouping-segment :brand :segment :sum-quantity])
                    (r/group-by [[:grouping-sets [:brand] [:segment] []]])
                    (r/order-by [:brand :segment]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 300, :brand "ABC", :grouping-segment 1, :segment nil, :grouping-brand 0}
                  #:sales{:sum-quantity 400, :brand "XYZ", :grouping-segment 1, :segment nil, :grouping-brand 0}
                  #:sales{:sum-quantity 500, :brand nil, :grouping-segment 0, :segment "Basic", :grouping-brand 1}
                  #:sales{:sum-quantity 200, :brand nil, :grouping-segment 0, :segment "Premium", :grouping-brand 1}
                  #:sales{:sum-quantity 700, :brand nil, :grouping-segment 1, :segment nil, :grouping-brand 1}])))

  (testing "Grouping function 2"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/extend :grouping-brand [:grouping :brand])
                    (r/extend :grouping-segment [:grouping :segment])
                    (r/select [:grouping-brand :grouping-segment :brand :segment :sum-quantity])
                    (r/group-by [[:grouping-sets [:brand] [:segment] []]])
                    (r/having [:= :grouping-brand 0])
                    (r/order-by [:brand :segment]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 300, :brand "ABC", :grouping-segment 1, :segment nil, :grouping-brand 0}
                  #:sales{:sum-quantity 400, :brand "XYZ", :grouping-segment 1, :segment nil, :grouping-brand 0}]))))

(deftest cube
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-cube/
  (testing "CUBE"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [[:cube :brand :segment]])
                    (r/order-by [:brand :segment]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 300, :brand "ABC", :segment nil}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 400, :brand "XYZ", :segment nil}
                  #:sales{:sum-quantity 500, :brand nil, :segment "Basic"}
                  #:sales{:sum-quantity 200, :brand nil, :segment "Premium"}
                  #:sales{:sum-quantity 700, :brand nil, :segment nil}])))

  (testing "Partial CUBE"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [:brand [:cube :segment]])
                    (r/order-by [:brand :segment]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 300, :brand "ABC", :segment nil}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 400, :brand "XYZ", :segment nil}]))))


(deftest rollup
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-rollup/
  (testing "ROLLUP #1"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [[:rollup :brand :segment]])
                    (r/order-by [:brand :segment]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 300, :brand "ABC", :segment nil}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 400, :brand "XYZ", :segment nil}
                  #:sales{:sum-quantity 700, :brand nil, :segment nil}])))

  (testing "ROLLUP #2"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [[:rollup :segment :brand]])
                    (r/order-by [:segment :brand]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 500, :brand nil, :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 200, :brand nil, :segment "Premium"}
                  #:sales{:sum-quantity 700, :brand nil, :segment nil}])))

  (testing "Partial ROLLUP"
    (let [sales (-> *env*
                    :sales
                    (r/extend-with-aggregate :sum-quantity [:sum :quantity])
                    (r/select [:brand :segment :sum-quantity])
                    (r/group-by [:segment [:rollup :brand]])
                    (r/order-by [:segment :brand]))
          res (select! *env* sales)]
      (fact
       res =in=> [#:sales{:sum-quantity 200, :brand "ABC", :segment "Basic"}
                  #:sales{:sum-quantity 300, :brand "XYZ", :segment "Basic"}
                  #:sales{:sum-quantity 500, :brand nil, :segment "Basic"}
                  #:sales{:sum-quantity 100, :brand "ABC", :segment "Premium"}
                  #:sales{:sum-quantity 100, :brand "XYZ", :segment "Premium"}
                  #:sales{:sum-quantity 200, :brand nil, :segment "Premium"}])))

  (testing "Rental ROLLUP"
    (let [rental (-> *env*
                     :rental
                     (r/extend :year [:extract :year :rental-date])
                     (r/extend :month [:extract :month :rental-date])
                     (r/extend :day [:extract :day :rental-date])
                     (r/extend-with-aggregate :rental-count [:count :rental-id])
                     (r/select [:year :month :day :rental-count])
                     (r/group-by [[:rollup :year :month :day]])
                     (r/order-by [:year :month :day]))
          res (-> (select! *env* rental)
                  (subvec 0 10))]
      (fact
       res =in=> [#:rental{:day 24.0, :month 5.0, :rental-count 2, :year 2005.0}
                  #:rental{:day 25.0, :month 5.0, :rental-count 136, :year 2005.0}
                  #:rental{:day 26.0, :month 5.0, :rental-count 175, :year 2005.0}
                  #:rental{:day 27.0, :month 5.0, :rental-count 168, :year 2005.0}
                  #:rental{:day 28.0, :month 5.0, :rental-count 194, :year 2005.0}
                  #:rental{:day 29.0, :month 5.0, :rental-count 156, :year 2005.0}
                  #:rental{:day 30.0, :month 5.0, :rental-count 155, :year 2005.0}
                  #:rental{:day 31.0, :month 5.0, :rental-count 170, :year 2005.0}
                  #:rental{:day nil, :month 5.0, :rental-count 1156, :year 2005.0}
                  #:rental{:day 14.0, :month 6.0, :rental-count 2, :year 2005.0}]))))

(deftest subquery
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-subquery/
  (testing "PostgreSQL subquery"
    (let [{:keys [film]} *env*
          avg-film-rental-rate (-> film
                                   (r/extend :avg-film-rental-rate [:avg :rental-rate])
                                   (r/select [:avg-film-rental-rate]))
          films (-> film
                    (r/select [:film-id :title :rental-rate])
                    (r/where [:> :rental-rate avg-film-rental-rate])
                    (r/order-by [:rental-rate])) ;; add order-by for consistent test results
          res (-> (select! *env* films)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:title "KRAMER CHOCOLATE", :film-id 503, :rental-rate 2.99M}
                  #:film{:title "LABYRINTH LEAGUE", :film-id 505, :rental-rate 2.99M}
                  #:film{:title "BED HIGHBALL", :film-id 62, :rental-rate 2.99M}
                  #:film{:title "ALABAMA DEVIL", :film-id 9, :rental-rate 2.99M}
                  #:film{:title "DREAM PICKUP", :film-id 252, :rental-rate 2.99M}
                  #:film{:title "BERETS AGENT", :film-id 67, :rental-rate 2.99M}
                  #:film{:title "LEATHERNECKS DWARFS", :film-id 513, :rental-rate 2.99M}
                  #:film{:title "LEBOWSKI SOLDIERS", :film-id 514, :rental-rate 2.99M}
                  #:film{:title "DRIVER ANNIE", :film-id 254, :rental-rate 2.99M}
                  #:film{:title "CHANCE RESURRECTION", :film-id 135, :rental-rate 2.99M}])))

  (testing "PostgreSQL subquery with IN operator"
    (let [{:keys [film rental inventory]} *env*
          returned-films (-> rental
                             ;; Last argument is "join projection". It allows you to select subset of fields from the joined table
                             (r/inner-join inventory :inventory [:= :inventory-id :inventory/inventory-id] [:inventory/film-id])
                             (r/where [:between :return-date [:cast "2005-05-29" "timestamp"] [:cast "2005-05-30" "timestamp"]])
                             ;; Since we want to use this query in an IN operation, we need to return a table with one column
                             ;; which is :inventory/film-id. Therefore we don't select anything from the rental table
                             (r/select []))
          films (-> film
                    (r/select [:film-id :title])
                    (r/where [:in :film-id returned-films])
                    (r/order-by [:title])) ;; add order-by for consistent test results
          res (-> (select! *env* films)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:title "ALIEN CENTER", :film-id 15}
                  #:film{:title "AMADEUS HOLY", :film-id 19}
                  #:film{:title "ATTRACTION NEWTON", :film-id 45}
                  #:film{:title "BAKED CLEOPATRA", :film-id 50}
                  #:film{:title "BALLROOM MOCKINGBIRD", :film-id 52}
                  #:film{:title "BANGER PINOCCHIO", :film-id 54}
                  #:film{:title "BETRAYED REAR", :film-id 68}
                  #:film{:title "BINGO TALENTED", :film-id 73}
                  #:film{:title "BLUES INSTINCT", :film-id 83}
                  #:film{:title "BORROWERS BEDAZZLED", :film-id 89}])))

  (testing "PostgreSQL subquery with IN operator"
    (let [{:keys [film rental inventory]} *env*
          returned-films (-> rental
                             ;; Last argument is "join projection". It allows you to select subset of fields from the joined table
                             (r/inner-join inventory :inventory [:= :inventory-id :inventory/inventory-id] [:inventory/film-id])
                             (r/where [:between :return-date [:cast "2005-05-29" "timestamp"] [:cast "2005-05-30" "timestamp"]])
                             ;; Since we want to use this query in an IN operation, we need to return a table with one column
                             ;; which is :inventory/film-id. Therefore we don't select anything from the rental table
                             (r/select []))
          films (-> film
                    (r/select [:film-id :title])
                    (r/where [:in :film-id returned-films])
                    (r/order-by [:title])) ;; add order-by for consistent test results
          res (-> (select! *env* films)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:title "ALIEN CENTER", :film-id 15}
                  #:film{:title "AMADEUS HOLY", :film-id 19}
                  #:film{:title "ATTRACTION NEWTON", :film-id 45}
                  #:film{:title "BAKED CLEOPATRA", :film-id 50}
                  #:film{:title "BALLROOM MOCKINGBIRD", :film-id 52}
                  #:film{:title "BANGER PINOCCHIO", :film-id 54}
                  #:film{:title "BETRAYED REAR", :film-id 68}
                  #:film{:title "BINGO TALENTED", :film-id 73}
                  #:film{:title "BLUES INSTINCT", :film-id 83}
                  #:film{:title "BORROWERS BEDAZZLED", :film-id 89}])))

  (testing "PostgreSQL subquery with EXISTS operator"
    (let [{:keys [customer payment]} *env*
          ;; This example requires a correlated subquery. We want to query 
          ;; payments table for each row in customers table. You can express this
          ;; in Penkala by using `r/with-parent` and `[:parent-scope ...]` which lets
          ;; you to access the columns from the parent scope in the inner subquery
          payments (-> payment
                       (r/with-parent customer)
                       (r/select [:payment-id])
                       (r/where [:= :customer-id [:parent-scope :customer-id]]))
          customers (-> customer
                        (r/select [:first-name :last-name])
                        (r/order-by [:last-name :first-name])
                        (r/where [:exists payments]))
          res (-> (select! *env* customers)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "ABNEY", :first-name "RAFAEL"}
                  #:customer{:last-name "ADAM", :first-name "NATHANIEL"}
                  #:customer{:last-name "ADAMS", :first-name "KATHLEEN"}
                  #:customer{:last-name "ALEXANDER", :first-name "DIANA"}
                  #:customer{:last-name "ALLARD", :first-name "GORDON"}
                  #:customer{:last-name "ALLEN", :first-name "SHIRLEY"}
                  #:customer{:last-name "ALVAREZ", :first-name "CHARLENE"}
                  #:customer{:last-name "ANDERSON", :first-name "LISA"}
                  #:customer{:last-name "ANDREW", :first-name "JOSE"}
                  #:customer{:last-name "ANDREWS", :first-name "IDA"}]))))

(deftest any
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-any/
  (testing "ANY"
    (let [{:keys [film film-category]} *env*
          longest-film-by-category (-> film
                                       (r/extend :longest-length [:max :length])
                                       (r/inner-join film-category :film-category [:using :film-id] [])
                                       (r/select [:longest-length])
                                       (r/group-by [:film-category/category-id]))
          films (-> film
                    (r/select [:title])
                    (r/where [:>= :length [:any longest-film-by-category]])
                    (r/order-by [:title])) ;; Add order-by for consistent test results
          res (-> (select! *env* films)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:title "ALLEY EVOLUTION"}
                  #:film{:title "ANALYZE HOOSIERS"}
                  #:film{:title "ANONYMOUS HUMAN"}
                  #:film{:title "BAKED CLEOPATRA"}
                  #:film{:title "BORN SPINAL"}
                  #:film{:title "CASUALTIES ENCINO"}
                  #:film{:title "CATCH AMISTAD"}
                  #:film{:title "CAUSE DATE"}
                  #:film{:title "CHICAGO NORTH"}
                  #:film{:title "CONFIDENTIAL INTERVIEW"}])))
  (testing "ANY vs. IN"
    (let [{:keys [film film-category category]} *env*
          action-drama-category (-> category
                                    (r/select [:category-id])
                                    (r/where [:or [:= :name "Action"] [:= :name "Drama"]]))
          films-any (-> film
                        (r/select [:title])
                        (r/inner-join film-category :film-category [:using :film-id] [:film-category/category-id])
                        (r/where [:= :film-category/category-id [:any action-drama-category]])
                        (r/order-by [:title])) ;; Add order-by for consistent test results
          films-in (-> film
                       (r/select [:title])
                       (r/inner-join film-category :film-category [:using :film-id] [:film-category/category-id])
                       (r/where [:in :film-category/category-id action-drama-category])
                       (r/order-by [:title])) ;; Add order-by for consistent test results
          res-films-any (-> (select! *env* films-any)
                            (subvec 0 10))
          res-films-in (-> (select! *env* films-in)
                           (subvec 0 10))]
      (facts
       res-films-any => res-films-in
       res-films-any => [#:film{:title "AMADEUS HOLY", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "AMERICAN CIRCUS", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "ANTITRUST TOMATOES", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "APOLLO TEEN", :film-category [#:film-category{:category-id 7}]}
                         #:film{:title "ARK RIDGEMONT", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "BAREFOOT MANCHURIAN", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "BEAUTY GREASE", :film-category [#:film-category{:category-id 7}]}
                         #:film{:title "BEETHOVEN EXORCIST", :film-category [#:film-category{:category-id 7}]}
                         #:film{:title "BERETS AGENT", :film-category [#:film-category{:category-id 1}]}
                         #:film{:title "BLADE POLISH", :film-category [#:film-category{:category-id 7}]}]
       res-films-in => [#:film{:title "AMADEUS HOLY", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "AMERICAN CIRCUS", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "ANTITRUST TOMATOES", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "APOLLO TEEN", :film-category [#:film-category{:category-id 7}]}
                        #:film{:title "ARK RIDGEMONT", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "BAREFOOT MANCHURIAN", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "BEAUTY GREASE", :film-category [#:film-category{:category-id 7}]}
                        #:film{:title "BEETHOVEN EXORCIST", :film-category [#:film-category{:category-id 7}]}
                        #:film{:title "BERETS AGENT", :film-category [#:film-category{:category-id 1}]}
                        #:film{:title "BLADE POLISH", :film-category [#:film-category{:category-id 7}]}]))))

(deftest all
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-all/
  (testing "ALL"
    (let [{:keys [film]} *env*
          avg-length-by-rating (-> film
                                   (r/extend-with-aggregate :avg-length [:round [:avg :length] [:cast 2 "int"]])
                                   (r/select [:avg-length])
                                   (r/group-by [:rating]))
          films (-> film
                    (r/select [:film-id :title :length])
                    (r/where [:> :length [:all avg-length-by-rating]])
                    (r/order-by [:length]))
          res (-> (select! *env* films)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:title "BRANNIGAN SUNRISE", :film-id 93, :length 121}
                  #:film{:title "PARIS WEEKEND", :film-id 658, :length 121}
                  #:film{:title "HARRY IDAHO", :film-id 403, :length 121}
                  #:film{:title "ARIZONA BANG", :film-id 37, :length 121}
                  #:film{:title "DANGEROUS UPTOWN", :film-id 207, :length 121}
                  #:film{:title "JUMANJI BLADE", :film-id 490, :length 121}
                  #:film{:title "PURE RUNNER", :film-id 704, :length 121}
                  #:film{:title "BOOGIE AMELIE", :film-id 86, :length 121}
                  #:film{:title "CHICKEN HELLFIGHTERS", :film-id 142, :length 122}
                  #:film{:title "CONFUSED CANDLES", :film-id 175, :length 122}]))))

(deftest exists
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-all/
  (testing "Find customers who have at least one payment whose amount is greater than 11."
    (let [{:keys [customer payment]} *env*
          ;; This example requires a correlated subquery. We want to query 
          ;; payments table for each row in customers table. You can express this
          ;; in Penkala by using `r/with-parent` and `[:parent-scope ...]` which lets
          ;; you to access the columns from the parent scope in the inner subquery
          payments (-> payment
                       (r/with-parent customer)
                       (r/select [:payment-id])
                       (r/where  [:and
                                  [:= :customer-id [:parent-scope :customer-id]]
                                  [:> :amount 11]]))
          customers (-> customer
                        (r/select [:first-name :last-name])
                        (r/where [:exists payments])
                        (r/order-by [:first-name :last-name]))
          res (-> (select! *env* customers)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "AUSTIN", :first-name "ALMA"}
                  #:customer{:last-name "JACKSON", :first-name "KAREN"}
                  #:customer{:last-name "ARSENAULT", :first-name "KENT"}
                  #:customer{:last-name "BARFIELD", :first-name "NICHOLAS"}
                  #:customer{:last-name "MCCRARY", :first-name "RICHARD"}
                  #:customer{:last-name "SCHMIDT", :first-name "ROSEMARY"}
                  #:customer{:last-name "GILBERT", :first-name "TANYA"}
                  #:customer{:last-name "ROUSH", :first-name "TERRANCE"}
                  #:customer{:last-name "SIMS", :first-name "VANESSA"}
                  #:customer{:last-name "GIBSON", :first-name "VICTORIA"}])))
  (testing "NOT EXISTS example"
    (let [{:keys [customer payment]} *env*
          ;; This example requires a correlated subquery. We want to query 
          ;; payments table for each row in customers table. You can express this
          ;; in Penkala by using `r/with-parent` and `[:parent-scope ...]` which lets
          ;; you to access the columns from the parent scope in the inner subquery
          payments (-> payment
                       (r/with-parent customer)
                       (r/select [:payment-id])
                       (r/where  [:and
                                  [:= :customer-id [:parent-scope :customer-id]]
                                  [:> :amount 11]]))
          customers (-> customer
                        (r/select [:first-name :last-name])
                        (r/where [:not [:exists payments]])
                        (r/order-by [:first-name :last-name]))
          res (-> (select! *env* customers)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY", :first-name "AARON"}
                  #:customer{:last-name "GOOCH", :first-name "ADAM"}
                  #:customer{:last-name "CLARY", :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP", :first-name "AGNES"}
                  #:customer{:last-name "KAHN", :first-name "ALAN"}
                  #:customer{:last-name "CROUSE", :first-name "ALBERT"}
                  #:customer{:last-name "HENNING", :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM", :first-name "ALEX"}
                  #:customer{:last-name "FENNELL", :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS", :first-name "ALFRED"}]))))

(deftest cte
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-cte/
  (testing "A simple PostgreSQL CTE example"
    (let [{:keys [film]} *env*
          film-cte (r/as-cte
                    (-> film
                        (r/extend :length-description
                          [:case
                           [:when [:< :length [:cast 30 "int"]]
                            "Short"]
                           [:when [:< :length [:cast 90 "int"]]
                            "Medium"]
                           "Long"])
                        (r/select [:film-id :title :length-description])))
          query (-> film-cte
                    (r/where [:= :length-description "Long"])
                    (r/order-by [:title]))
          res (-> (select! *env* query)
                  (subvec 0 10))]
      (fact
       res =in=> [#:film{:length-description "Long", :title "AFFAIR PREJUDICE", :film-id 4}
                  #:film{:length-description "Long", :title "AFRICAN EGG", :film-id 5}
                  #:film{:length-description "Long", :title "AGENT TRUMAN", :film-id 6}
                  #:film{:length-description "Long", :title "ALABAMA DEVIL", :film-id 9}
                  #:film{:length-description "Long", :title "ALAMO VIDEOTAPE", :film-id 11}
                  #:film{:length-description "Long", :title "ALASKA PHANTOM", :film-id 12}
                  #:film{:length-description "Long", :title "ALI FOREVER", :film-id 13}
                  #:film{:length-description "Long", :title "ALICE FANTASIA", :film-id 14}
                  #:film{:length-description "Long", :title "ALLEY EVOLUTION", :film-id 16}
                  #:film{:length-description "Long", :title "AMADEUS HOLY", :film-id 19}])))

  (testing "Joining a CTE with a table example"
    (let [{:keys [rental staff]} *env*
          rental-cte (r/as-cte
                      (-> rental
                          (r/extend-with-aggregate :rental-count [:count :rental-id])
                          (r/select [:staff-id :rental-count])
                          (r/group-by [:staff-id])))
          query (-> staff
                    (r/select [:first-name :last-name :staff-id])
                    (r/inner-join rental-cte :rental-cte [:using :staff-id]))
          res  (select! *env* query)]
      (fact
       res =in=> [#:staff{:last-name "Hillyer",
                          :first-name "Mike",
                          :staff-id 1,
                          :rental-cte [#:rental{:rental-count 8040, :staff-id 1}]}
                  #:staff{:last-name "Stephens",
                          :first-name "Jon",
                          :staff-id 2,
                          :rental-cte [#:rental{:rental-count 8004, :staff-id 2}]}])))

  (testing "Using CTE with a window function example"
    (let [{:keys [film]} *env*
          film-cte (r/as-cte
                    (-> film
                        (r/extend-with-window :length-rank [:rank] [:rating] [[:length :desc]])
                        (r/select [:title :rating :length :length-rank])))
          query (-> film-cte
                    (r/where [:= :length-rank 1])
                    (r/order-by [:title])) ;; Add order-by for consistent test results
          res (select! *env* query)]
      (fact
       res =in=> [#:film{:length-rank 1, :title "CHICAGO NORTH", :length 185, :rating "PG-13"}
                  #:film{:length-rank 1, :title "CONTROL ANTHEM", :length 185, :rating "G"}
                  #:film{:length-rank 1, :title "CRYSTAL BREAKING", :length 184, :rating "NC-17"}
                  #:film{:length-rank 1, :title "DARN FORRESTER", :length 185, :rating "G"}
                  #:film{:length-rank 1, :title "GANGS PRIDE", :length 185, :rating "PG-13"}
                  #:film{:length-rank 1, :title "HOME PITY", :length 185, :rating "R"}
                  #:film{:length-rank 1, :title "KING EVOLUTION", :length 184, :rating "NC-17"}
                  #:film{:length-rank 1, :title "MUSCLE BRIGHT", :length 185, :rating "G"}
                  #:film{:length-rank 1, :title "POND SEATTLE", :length 185, :rating "PG-13"}
                  #:film{:length-rank 1, :title "SOLDIERS EVOLUTION", :length 185, :rating "R"}
                  #:film{:length-rank 1, :title "SONS INTERVIEW", :length 184, :rating "NC-17"}
                  #:film{:length-rank 1, :title "SORORITY QUEEN", :length 184, :rating "NC-17"}
                  #:film{:length-rank 1, :title "SWEET BROTHERHOOD", :length 185, :rating "R"}
                  #:film{:length-rank 1, :title "WORST BANGER", :length 185, :rating "PG"}]))))

(deftest recursive-query
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-recursive-query/
  (let [{:keys [employees-2]} *env*
        subordinates (r/as-cte
                      (-> employees-2
                          (r/where [:= :employee-id [:cast 2 "int"]]))
                      (union [subordinates]
                             (-> employees-2
                                 (r/inner-join subordinates :subordinates [:= :manager-id :subordinates/employee-id] []))))
        res (select! *env* subordinates)]
    (fact
     res =in=> [#:employees-2{:manager-id 1, :full-name "Megan Berry", :employee-id 2}
                #:employees-2{:manager-id 2, :full-name "Bella Tucker", :employee-id 6}
                #:employees-2{:manager-id 2, :full-name "Ryan Metcalfe", :employee-id 7}
                #:employees-2{:manager-id 2, :full-name "Max Mills", :employee-id 8}
                #:employees-2{:manager-id 2, :full-name "Benjamin Glover", :employee-id 9}
                #:employees-2{:manager-id 7, :full-name "Piers Paige", :employee-id 16}
                #:employees-2{:manager-id 7, :full-name "Ryan Henderson", :employee-id 17}
                #:employees-2{:manager-id 8, :full-name "Frank Tucker", :employee-id 18}
                #:employees-2{:manager-id 8, :full-name "Nathan Ferguson", :employee-id 19}
                #:employees-2{:manager-id 8, :full-name "Kevin Rampling", :employee-id 20}])))

(deftest recursive-query-2
  ;; https://towardsdatascience.com/recursive-sql-queries-with-postgresql-87e2a453f1b
  (testing "Degrees of separation"
    (let [{:keys [employees-3]} *env*
          employees-3-cte (r/as-cte
                           (-> employees-3
                               (r/extend :degree 0)
                               (r/select [:id :degree])
                               (r/where [:= :id 1]))
                           (union-all [rec]
                                      (-> employees-3
                                          (r/inner-join rec :rec [:= :manager-id :rec/id] [])
                                          (r/extend :degree ["+" :rec/degree 1])
                                          (r/select [:id :degree]))))
          query (-> employees-3
                    (r/inner-join employees-3-cte :rec [:= :id :rec/id] [])
                    (r/extend :separation [:concat "John and " :name " are separated by "
                                           [:to-char :rec/degree "9"] " degrees of separation"])
                    (r/where [:= :name "George"]))
          res (select-one! *env* query)]
      (fact
       res => #:employees-3{:name "George", :manager-id 5, :separation "John and George are separated by  3 degrees of separation", :id 4, :salary 1800, :job "Developer"})))

  (testing "Progression"
    (let [{:keys [employees-3]} *env*
          employees-3-cte (r/as-cte
                           (-> employees-3
                               (r/extend :level 0)
                               (r/select [:id :manager-id :job :level])
                               (r/where [:= :id 4]))
                           (union-all [rec]
                                      (-> employees-3
                                          (r/inner-join rec :rec [:= :id :rec/manager-id] [])
                                          (r/extend :level ["+" :rec/level 1])
                                          (r/select [:id :manager-id :job :level]))))
          query (-> employees-3-cte
                    (r/extend-with-aggregate :path [:string-agg :job " > " [:order-by [:level :asc]]])
                    (r/select [:path]))
          res (select-one! *env* query)]
      (fact
       res => #:employees-3{:path "Developer > Manager > VP > CEO"})))

  (testing "Recursion on Graphs"
    (let [{:keys [edges]} *env*
          paths (r/as-cte
                 (-> edges
                     (r/rename :src :source)
                     (r/extend :path [:array :source :dest])
                     (r/select [:source :dest :path]))
                 (union-all [paths]
                            (-> edges
                                (r/inner-join paths :p [:and [:= :src :p/dest] [:<> :dest [:all :p/path]]] [])
                                (r/extend :source :p/source)
                                (r/extend :path [:array-cat :p/path [:array :dest]])

                                (r/select [:source :dest :path]))))
          res (select! *env* paths)]
      (fact
       res => [#:edges{:path [1 2], :source 1, :dest 2}
               #:edges{:path [2 3], :source 2, :dest 3}
               #:edges{:path [2 4], :source 2, :dest 4}
               #:edges{:path [3 4], :source 3, :dest 4}
               #:edges{:path [4 1], :source 4, :dest 1}
               #:edges{:path [3 5], :source 3, :dest 5}
               #:edges{:path [4 1 2], :source 4, :dest 2}
               #:edges{:path [1 2 3], :source 1, :dest 3}
               #:edges{:path [1 2 4], :source 1, :dest 4}
               #:edges{:path [2 3 4], :source 2, :dest 4}
               #:edges{:path [2 3 5], :source 2, :dest 5}
               #:edges{:path [2 4 1], :source 2, :dest 1}
               #:edges{:path [3 4 1], :source 3, :dest 1}
               #:edges{:path [3 4 1 2], :source 3, :dest 2}
               #:edges{:path [4 1 2 3], :source 4, :dest 3}
               #:edges{:path [1 2 3 4], :source 1, :dest 4}
               #:edges{:path [1 2 3 5], :source 1, :dest 5}
               #:edges{:path [2 3 4 1], :source 2, :dest 1}
               #:edges{:path [4 1 2 3 5], :source 4, :dest 5}]))))