Feature: Operator-initiated node draining
  Scenario: Drain a node and preserve its data on the remaining peer
    Given two ready peers with an authorized operator
    And single-replica data has been written across both peers
    When the operator requests that the node drain
    Then the command acknowledges asynchronous draining initiation
    And the draining node reaches idle and the remaining peer observes its completion
    And all written data remains readable after the drained process is stopped

  Scenario: Reject draining the only node
    Given a ready node with an authorized operator
    When the operator requests that the node drain
    Then the command rejects draining without another node

  Scenario: Cancel a blocked drain and retain all data
    Given two ready peers with an authorized operator
    And single-replica data has been written across both peers
    And the remaining peer is stopped
    When the operator requests and cancels draining
    Then the draining node returns to ready
    And the restarted peer can read all written data
