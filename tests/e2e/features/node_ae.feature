Feature: Operator view of anti-entropy delivery
  Scenario: A settled node owes nothing and rejects a forged cursor
    Given a ready anti-entropy node holding one key
    When the operator reads the anti-entropy status
    Then the worker is running and no target is owed data
    And the obligation listing is empty
    And a forged obligation cursor is rejected as invalid input
