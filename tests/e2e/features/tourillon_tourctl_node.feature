Feature: tourctl node commands
  End-to-end behaviors for tourctl node status, cluster status and node join commands.

  Scenario: Display help for tourctl node status
    When I request help for the tourctl node status command
    Then the node command returns help text

  Scenario: Display help for tourctl node join
    When I request help for the tourctl node join command
    Then the node command returns help text

  Scenario: Start a node with tourillon and show its status with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl node status command against peer endpoint
    Then the node status command exits with zero code
    And the node status output includes the running node id

  Scenario: Start a node with tourillon and check its health with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl node health command against peer endpoint
    Then the node health command exits with zero code
    And the node health output reports the running node as healthy

  Scenario: Start a node with tourillon and show the cluster with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl cluster status command against peer endpoint
    Then the node status command exits with zero code
    And the cluster status output is the view from the running node

  Scenario: Start a node with tourillon and inspect its member report
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl cluster member command using the client context
    Then the node status command exits with zero code
    And the cluster member output describes the running node

  Scenario: Start a node with tourillon and trigger join with tourctl
    Given a CA has been generated for tourctl node commands
    And node and client configs have been generated for a running node command target
    And a tourillon node is running for node commands
    When I run the tourctl node join command against peer endpoint
    Then the node join command exits with zero code
    And the node join output confirms the node was already ready
