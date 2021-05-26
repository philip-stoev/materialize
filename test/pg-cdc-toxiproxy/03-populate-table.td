# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE ten (f1 INTEGER);
ALTER TABLE ten REPLICA IDENTITY FULL;
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

CREATE TABLE t1 (f1 INTEGER, f2 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t2 (f1 INTEGER, f2 TEXT);
ALTER TABLE t2 REPLICA IDENTITY FULL;

INSERT INTO t2 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t3 (f1 INTEGER, f2 TEXT);
ALTER TABLE t3 REPLICA IDENTITY FULL;

INSERT INTO t3 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t4 (f1 INTEGER, f2 TEXT);
ALTER TABLE t4 REPLICA IDENTITY FULL;

INSERT INTO t4 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t5 (f1 INTEGER, f2 TEXT);
ALTER TABLE t5 REPLICA IDENTITY FULL;

INSERT INTO t5 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t6 (f1 INTEGER, f2 TEXT);
ALTER TABLE t6 REPLICA IDENTITY FULL;

INSERT INTO t6 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t7 (f1 INTEGER, f2 TEXT);
ALTER TABLE t7 REPLICA IDENTITY FULL;

INSERT INTO t7 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t8 (f1 INTEGER, f2 TEXT);
ALTER TABLE t8 REPLICA IDENTITY FULL;

INSERT INTO t8 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t9 (f1 INTEGER, f2 TEXT);
ALTER TABLE t9 REPLICA IDENTITY FULL;

INSERT INTO t9 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

CREATE TABLE t10 (f1 INTEGER, f2 TEXT);
ALTER TABLE t10 REPLICA IDENTITY FULL;

INSERT INTO t10 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;


