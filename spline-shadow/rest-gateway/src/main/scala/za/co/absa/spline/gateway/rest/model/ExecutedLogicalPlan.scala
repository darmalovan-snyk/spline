/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.gateway.rest.model

import java.util.UUID

import za.co.absa.spline.gateway.rest.model.ExecutedLogicalPlan._

case class ExecutedLogicalPlan
(
  app: AppInfo,
  execution: ExecutionInfo,
  dag: LogicalPlan
)

object ExecutedLogicalPlan {
  type OperationID = UUID

  case class LogicalPlan(nodes: Seq[Operation], edges: Seq[Transition]) extends Graph {
    override type Node = Operation
    override type Edge = Transition
  }

  case class Operation(_id: OperationID, _type: String, name: String) extends Graph.Node {
    override type Id = OperationID
  }

  case class Transition(_from: OperationID, _to: OperationID) extends Graph.Edge {
    override type JointId = OperationID
  }

}