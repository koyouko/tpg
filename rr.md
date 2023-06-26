we have observed that most of our production brokers are reaching the maximum limit of FETCH_SESSION slots (max.incremental.fetch.session.cache.slots). To mitigate this issue and improve our consumer efficiency,  an increase in the FETCH_SESSION slots from the current default value 1000 to 10000. We believe that this adjustment will better align with our production needs and alleviate the current bottleneck.

In addition, we would also like to propose an adjustment to the group initial rebalancing delay (group.initial.rebalancing.delay.ms). Currently, it is set to 0 milliseconds, resulting in immediate rebalancing upon any consumer joining or leaving a group. To provide a more stable and controlled rebalancing process, update delay to 3 seconds (group.initial.rebalancing.delay.ms=3000).

To implement these changes, we kindly ask you to add the following new broker properties:

max.incremental.fetch.session.cache.slots=10000
group.initial.rebalancing.delay.ms=3000


To monitor the FETCH_SESSION slots, we rely on the JMX metric "kafka.server:type=FetchSessionCache,name=NumIncrementalFetchSessions." This metric provides valuable insights into the usage and saturation level of FETCH_SESSION slots, helping us gauge the effectiveness of the proposed adjustment.

Link
I