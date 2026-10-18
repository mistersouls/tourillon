Feature: Operate a real Tourillon database through its command-line tools
  Each scenario uses its own generated certificates, configurations and storage.
  Executables may be supplied independently or discovered on PATH.
  Explicit executables run outside the repository with an empty PATH and no
  Python, virtual-environment or dynamic-library search overrides.
  Their locale is forced to C to verify Unicode independently of the host locale.
  Portable executables must include their complete runtime bundle.
  Use --journey-artifacts DIR to retain per-scenario Markdown reports and runtime
  logs.
  Exported files exclude generated configuration, certificates and private keys.

  Scenario: Join a second node and preserve data through routing and mutations
    Given an authorized single-node database
    And reference data written before any other node joins
    When I configure and start node B and request that it join node A
    Then both nodes converge to the same stable ready membership
    And pre-join data including transferred partitions is readable through both nodes
    When I write through B, update through A and delete through B
    Then both nodes return the exact surviving values and no deleted value

  Scenario: Restart preserves identity, values and deletions
    Given an authorized single-node database
    And a surviving value and a deleted value
    When I stop and restart the node using the same configuration and storage
    Then the node retains its identity, ring tokens, surviving value and deletion

  Scenario: Typed values round trip without data loss
    Given an authorized single-node database
    When I write explicit string, Unicode, integer, boolean, JSON and binary file values
    Then every typed value is returned exactly with its original type

  Scenario: Namespaces isolate the same key
    Given an authorized single-node database
    When I write the same key in two namespaces and delete it from one
    Then only that namespace loses its value and the default namespace stays empty

  Scenario: An under-replicated write is resumable from the handle it returns
    Given an authorized single-node database
    When I write a value demanding more replicas than the database holds
    Then the write fails but the value is readable and the handle resumes it

  Scenario: Invalid node configuration fails without leaving a server
    Given a generated node and authorized client configuration
    When I start the node with an invalid configuration
    Then startup fails explicitly and neither endpoint remains listening

  Scenario: An unreachable node produces a bounded explicit error
    Given a generated node and authorized client configuration
    When I probe the configured node without starting it
    Then the command fails within its deadline with an unreachable-node error

  Scenario: A client signed by an untrusted authority is rejected
    Given an authorized single-node database
    And a client that trusts the server but is signed by a different authority
    When the untrusted client probes both endpoints
    Then both requests fail and the authorized client can still use the database
