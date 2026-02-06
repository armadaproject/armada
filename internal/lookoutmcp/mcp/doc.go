// Package mcp provides core Model Context Protocol types and utilities.
//
// It implements the MCP wire protocol for communication between MCP servers
// and clients over JSON-RPC 2.0.
//
// The Model Context Protocol (MCP) is a standardised protocol for integrating
// LLM applications with external data sources and tools. This package provides
// the fundamental types and structures needed to implement an MCP server.
//
// # Protocol Overview
//
// MCP uses JSON-RPC 2.0 as its transport layer. All communication follows the
// request-response pattern where:
//   - Requests have a method, optional params, and an ID for matching responses
//   - Responses contain either a result or an error, with the same ID as the request
//   - Notifications are one-way messages without an ID
//
// # Server Capabilities
//
// An MCP server can expose three types of capabilities:
//   - Tools: Functions that can be invoked by the LLM
//   - Prompts: Pre-defined prompt templates
//   - Resources: Data sources that can be queried
//
// The Lookout MCP server currently implements Tools capability for querying
// job information from the Armada Lookout database.
//
// # Available Tools
//
// The server provides the following tools:
//   - search_jobs: Search for jobs with flexible filters
//   - get_job_details: Get detailed information about a specific job
//   - get_job_spec: Get the full job specification
//   - get_job_error: Get error messages for failed jobs
//   - get_job_run_error: Get error messages for specific job runs
//   - get_job_run_debug: Get debug information for job runs
//   - get_job_commands: Get kubectl commands for job management
//   - get_job_links: Get external links related to jobs
//   - get_lookout_ui_link: Get the Lookout UI link for a job
//   - get_lookout_ui_link: Get the Lookout UI link for a job
//
// # Configuration
//
// To enable the MCP endpoint, set mcpEnabled: true in the Lookout configuration.
// The server will expose an /mcp endpoint that accepts POST requests with
// JSON-RPC 2.0 formatted messages.
//
// Example configuration:
//
//	mcpEnabled: true
//	lookoutUIBaseUrl: "http://localhost:3000"
//
// # Protocol Version
//
// This implementation follows MCP protocol version 2024-11-05.
package mcp
