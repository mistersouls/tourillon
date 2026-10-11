Feature: tourillon node start command
  End-to-end behaviors for node startup.

  Scenario: Display help for node start
    When I request help for the tourillon node start command
    Then the command returns help text

  Scenario: Require config for node start
    When I run the tourillon node start command without a config file
    Then the command fails because config is missing

  Scenario: Start node from a generated config file
    Given a CA has been generated for the node start command
    And a node config file has been generated for node start with custom endpoints
    When I run the tourillon node start command
    Then the command reports node startup
    And the generated node config preserves the requested endpoints

