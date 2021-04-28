# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE ten (f1 INTEGER);
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 SELECT a1.f1 * a2.f1 * a3.f1 * a4.f1 * a5.f1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> CREATE MATERIALIZED SOURCE "t1"
  FROM POSTGRES
  HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source'
  TABLE t1 (f1 INTEGER)
