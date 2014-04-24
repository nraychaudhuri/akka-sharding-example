package sharding.example

import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}

object JobStreamRenderConfig extends MultiNodeConfig {

}

class JobStreamRenderSpec extends MultiNodeSpec(JobStreamRenderConfig) {

  override val initialParticipants = 1

}
