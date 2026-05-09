// Package lookout provides the Lookout UI backend service for viewing and querying Armada jobs.
//
// Lookout serves a REST API for job queries, group-by operations, and retrieving job details.
// It also provides an optional MCP (Model Context Protocol) endpoint for LLM integrations,
// which can be enabled via the MCPEnabled configuration flag.
//
// The MCP endpoint exposes tools for:
//   - Getting job details
//   - Retrieving job specifications
//   - Listing available kubectl commands for jobs
//   - Getting external links related to jobs
//   - Generating Lookout UI links for jobs
package lookout
