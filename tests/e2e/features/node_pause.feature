Feature: Pause and resume a node
  Scenario: Pause a node without draining its data and resume after catch-up
    Given a two-peer cluster with data on the pause target
    When the operator pauses the target node
    Then the target refuses client operations and reports paused
    When the operator resumes the target and waits
    Then the target reaches ready and the data remains readable
