Feature: tourillon config ca command
  End-to-end behaviors for CA material generation.

  Scenario: Generate CA files in an empty output directory
    Given a clean CA output directory
    And valid CA command options
    When I run the tourillon config ca command
    Then the command reports generated CA files
    And the generated CA certificate exists
    And the generated CA private key exists

  Scenario: Refuse overwrite when files already exist
    Given an output directory with existing CA files
    When I run the tourillon config ca command
    Then the command fails with a file exists error
    And the error message mentions overwrite refusal
