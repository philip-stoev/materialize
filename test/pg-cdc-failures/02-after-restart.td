# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT a1.f1 * a2.f1 * a3.f1 * a4.f1 * a5.f1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> SELECT COUNT(*) = 2000000 FROM t1;
true
