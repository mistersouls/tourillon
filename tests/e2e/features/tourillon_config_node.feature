Feature: tourillon config node command
  End-to-end behaviors for node config generation.

  Scenario: Generate node config in an empty output directory
    Given a CA has been generated in a temp directory
    And valid node command options
    When I run the tourillon config node command
    Then the command reports a generated node config file
    And the generated node config file exists
    And the generated node config uses the requested endpoints and seeds

  Scenario: Refuse overwrite when node config already exists
    Given a CA has been generated in a temp directory
    And a node config file already exists at the output path
    When I run the tourillon config node command
    Then the command fails with a file exists error
    And the error message mentions overwrite refusal

  Scenario: Fail when CA certificate is missing
    Given a CA has been generated in a temp directory
    And the CA certificate file has been deleted
    When I run the tourillon config node command
    Then the command exits with a non-zero code

