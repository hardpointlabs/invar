# MongoDB Wire Compatibility

This is package includes Invar's compatibility layer for people using MongoDB-compatible drivers in their applications.

This documents the goals and functional differences with the off the shelf MongoDB wire protocol and API.

## Development provenance

This is a ground-up cleanroom implementation of parts of the MongoDB wire protocol and associated API needed to support existing drivers.

The development workflow (e.g. AI agent chat sessions) are catalogued in the `papertrail` directory, committed and RFC 3161 timestamped to give externally verifiable point-in-time views of the development iterations.
