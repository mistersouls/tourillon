Feature: Operator-initiated node draining
  Scenario: Drain a node and preserve its data on the remaining peer
    Given two ready peers with an authorized operator
    And single-replica data has been written across both peers
    When the operator requests that the node leave
    Then the command acknowledges asynchronous draining initiation
    And the departing node reaches idle and the remaining peer observes its departure
    And all written data remains readable after the departed process is stopped

  Scenario: Reject departure of the only node
    Given a ready node with an authorized operator
    When the operator requests that the node leave
    Then the command rejects departure without another node
