Feature: tourillon config client command
  End-to-end behaviors for client config generation.

  Scenario: Generate client config in an empty output directory
    Given a CA has been generated for the client command
    And valid client command options
    When I run the tourillon config client command
    Then the command reports a generated client config file
    And the generated client config file exists

  Scenario: Refuse overwrite when client config already exists
    Given a CA has been generated for the client command
    And a client config file already exists at the output path
    When I run the tourillon config client command
    Then the command fails with a file exists error
    And the error message mentions overwrite refusal

