/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.test.RexImplicationCheckerFixtures.Fixture;
import org.apache.calcite.util.DateString;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;

/**
 * test class for testing predicateConstants.
 */
public class RexUtilTest {

  @Test public void testPredicateConstants() {
    Fixture f = new Fixture();
    RexBuilder rexB = f.rexBuilder;
    RelDataType dateColumnType = f.typeFactory.createTypeWithNullability(
        f.typeFactory.createSqlType(DATE), true);
    RexNode dateLiteral = rexB.makeLiteral(new DateString(2020, 12, 11),
        dateColumnType, false);
    RexNode dec20 = rexB.makeCall(
        IS_NOT_DISTINCT_FROM, rexB.makeInputRef(dateColumnType, 0), dateLiteral);
    ImmutableMap<RexNode, RexNode> constantMap = RexUtil.predicateConstants(RexNode.class,
        rexB, Arrays.asList(dec20));
    Assertions.assertFalse(constantMap.isEmpty());
    Assertions.assertEquals(dateLiteral,
        constantMap.get(rexB.makeInputRef(dateColumnType, 0)));
  }
}
