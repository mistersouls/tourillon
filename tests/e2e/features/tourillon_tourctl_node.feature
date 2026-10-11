Feature: tourctl node commands
  End-to-end behaviors for tourctl node inspect and node join commands.

  Scenario: Display help for tourctl node inspect
    When I request help for the tourctl node inspect command
    Then the node command returns help text

  Scenario: Display help for tourctl node join
    When I request help for the tourctl node join command
    Then the node command returns help text

  Scenario: Start a node with tourillon and inspect it with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl node inspect command against peer endpoint
    Then the node inspect command exits with zero code
    And the node inspect output includes the running node id

  Scenario: Start a node with tourillon and trigger join with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl node join command against peer endpoint
    Then the node join command exits with zero code
    And the node join output confirms the operation was triggered

