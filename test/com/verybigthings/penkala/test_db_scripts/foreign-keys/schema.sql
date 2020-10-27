CREATE SCHEMA public;

CREATE TABLE alpha (
  id SERIAL NOT NULL PRIMARY KEY,
  val TEXT
);

CREATE TABLE beta (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id INT,
  val TEXT,
  val2 TEXT,
  j JSONB,
  FOREIGN KEY (alpha_id) REFERENCES alpha(id)
);

ALTER TABLE beta DROP COLUMN val2;

CREATE TABLE gamma (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id_one INT NOT NULL,
  alpha_id_two INT,
  beta_id INT NOT NULL,
  val TEXT,
  j JSONB,
  FOREIGN KEY (alpha_id_one) REFERENCES alpha(id),
  FOREIGN KEY (alpha_id_two) REFERENCES alpha(id),
  FOREIGN KEY (beta_id) REFERENCES beta(id)
);

CREATE TABLE zeta (
  id SERIAL NOT NULL PRIMARY KEY,
  val TEXT
);

CREATE TABLE alpha_zeta (
  alpha_id INT NOT NULL,
  zeta_id INT NOT NULL
);

INSERT INTO alpha (val)
VALUES ('one'), ('two'), ('three'), ('four');

INSERT INTO beta (alpha_id, val, j)
VALUES
  (1, 'alpha one', null),
  (2, 'alpha two', null),
  (3, 'alpha three', null),
  (3, 'alpha three again', null),
  (null, 'not four', null),
  (null, 'not five', '{"x": {"y": "test"}}'::JSONB);

INSERT INTO gamma (alpha_id_one, alpha_id_two, beta_id, val, j)
VALUES
  (1, 1, 1, 'alpha one alpha one beta one', null),
  (1, 2, 2, 'alpha two alpha two beta two', null),
  (2, 3, 2, 'alpha two alpha three beta two again', null),
  (2, null, 3, 'alpha two (alpha null) beta three', null),
  (3, 1, 4, 'alpha three alpha one beta four', null),
  (4, null, 5, 'beta five', '{"z": {"a": "test"}}'::JSONB);

INSERT INTO zeta (val)
VALUES ('alpha one'), ('alpha one again'), ('alpha two');

INSERT INTO alpha_zeta (alpha_id, zeta_id)
VALUES (1, 1), (1, 2), (2, 3);

CREATE VIEW beta_view AS SELECT * FROM beta;

CREATE SCHEMA sch;

CREATE TABLE sch.delta (
  id SERIAL NOT NULL PRIMARY KEY,
  beta_id INT,
  val TEXT,
  FOREIGN KEY (beta_id) REFERENCES beta(id)
);

CREATE TABLE sch.epsilon (
  id SERIAL NOT NULL PRIMARY KEY,
  alpha_id INT,
  val TEXT,
  FOREIGN KEY (alpha_id) REFERENCES alpha(id)
);

INSERT INTO sch.delta (beta_id, val)
VALUES
  (1, 'beta one'),
  (2, 'beta two');

INSERT INTO sch.epsilon (alpha_id, val)
VALUES
  (1, 'alpha one'),
  (null, 'not two');
