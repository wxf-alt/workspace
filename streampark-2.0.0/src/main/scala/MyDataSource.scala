/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import QuickStartApp.User
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


class MyDataSource extends SourceFunction[User] {

  private[this] var isRunning = true

  override def cancel(): Unit = this.isRunning = false

  val random = new Random()

  var index = 0

  override def run(ctx: SourceFunction.SourceContext[User]): Unit = {
    while (isRunning && index <= 1000001) {
      index += 1
      val entity: User = new User("a" + System.currentTimeMillis(), random.nextInt(100), random.nextInt(1), "addr" + System.currentTimeMillis())
      ctx.collect(entity)
      Thread.sleep(random.nextInt(1000))
    }
  }

}