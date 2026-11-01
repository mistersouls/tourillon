Feature: Operator-initiated local historical collection
  Scenario: Estimate and collect local history while preserving current data
    Given a ready GC node with three versions of one key
    When the operator estimates collection and waits for the result
    Then the estimate reports one collectible version without deleting it
    When the operator collects history and waits for the result
    Then one version is collected and current data remains readable
    And node inspection shows the completed collection
    And a later estimate reports no collectible history

  Scenario: Reject a partition outside the node partition grid
    Given a ready GC node with three versions of one key
    When the operator requests collection of an invalid partition
    Then the command rejects the partition without a traceback
