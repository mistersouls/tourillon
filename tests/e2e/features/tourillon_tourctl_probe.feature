Feature: tourillon and tourctl interoperability
  End-to-end behaviors across tourillon and tourctl command lines.

  Scenario: Display help for tourctl probe ping
    When I request help for the tourctl probe ping command
    Then the tourctl command returns help text

  Scenario: Generate client config with tourillon and probe with tourctl
    Given a CA has been generated for tourctl probe
    And a client config has been generated for tourctl probe
    When I run the tourctl probe ping command against client endpoint
    Then the probe command exits with a non-zero code
    And the probe error reports an unreachable node

  Scenario: Start a node with tourillon and probe it with tourctl
    Given a CA has been generated for tourctl probe
    And node and client configs have been generated for a running probe target
    And a tourillon node is running for probe
    When I run the tourctl probe ping command against client endpoint
    Then the probe command exits with zero code
    And the probe output includes the running node id

  Scenario: Start a node with tourillon and probe peer endpoint with tourctl
    Given a CA has been generated for tourctl probe
    And node and client configs have been generated for a running probe target
    And a tourillon node is running for probe
    When I run the tourctl probe ping command against peer endpoint
    Then the probe command exits with zero code
    And the probe output includes the running node id
