package mcp

import (
	"encoding/json"
)

// ProtocolVersion is the MCP protocol version implemented by this package.
const ProtocolVersion = "2024-11-05"

// Request represents a JSON-RPC 2.0 request message.
type Request struct {
	JSONRPC string          `json:"jsonrpc"` // Must be "2.0"
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response represents a JSON-RPC 2.0 response message.
type Response struct {
	JSONRPC string `json:"jsonrpc"` // Must be "2.0"
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   *Error `json:"error,omitempty"`
}

// Notification represents a JSON-RPC 2.0 notification message (one-way, no ID).
type Notification struct {
	JSONRPC string          `json:"jsonrpc"` // Must be "2.0"
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Error represents a JSON-RPC 2.0 error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// InitializeParams contains parameters for the initialize method.
type InitializeParams struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ClientCapabilities `json:"capabilities"`
	ClientInfo      Implementation     `json:"clientInfo"`
}

// ClientCapabilities describes what features the client supports.
type ClientCapabilities struct {
	Experimental map[string]any `json:"experimental,omitempty"`
	Sampling     map[string]any `json:"sampling,omitempty"`
}

// Implementation describes the name and version of an MCP client or server.
type Implementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InitializeResult is returned from the initialize method.
type InitializeResult struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ServerCapabilities `json:"capabilities"`
	ServerInfo      Implementation     `json:"serverInfo"`
}

// ServerCapabilities describes what features the server supports.
type ServerCapabilities struct {
	Tools        *ToolsCapability     `json:"tools,omitempty"`
	Prompts      *PromptsCapability   `json:"prompts,omitempty"`
	Resources    *ResourcesCapability `json:"resources,omitempty"`
	Logging      map[string]any       `json:"logging,omitempty"`
	Experimental map[string]any       `json:"experimental,omitempty"`
}

// ToolsCapability indicates that the server supports tools.
type ToolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"` // Whether the tool list can change
}

// PromptsCapability indicates that the server supports prompts.
type PromptsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"` // Whether the prompt list can change
}

// ResourcesCapability indicates that the server supports resources.
type ResourcesCapability struct {
	Subscribe   bool `json:"subscribe,omitempty"`   // Whether clients can subscribe to resource updates
	ListChanged bool `json:"listChanged,omitempty"` // Whether the resource list can change
}

// ListToolsResult is returned from the tools/list method.
type ListToolsResult struct {
	Tools []Tool `json:"tools"`
}

// Tool describes a function that can be invoked by the LLM.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"` // JSON Schema for tool parameters
}

// CallToolParams contains parameters for the tools/call method.
type CallToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

// CallToolResult is returned from the tools/call method.
type CallToolResult struct {
	Content []Content `json:"content,omitempty"`
	IsError bool      `json:"isError,omitempty"`
}

// Content represents a piece of content returned by a tool.
// It can be text, data, or other types depending on the MimeType.
type Content struct {
	Type     string `json:"type"`               // "text", "image", "resource", etc.
	Text     string `json:"text,omitempty"`     // Text content
	Data     string `json:"data,omitempty"`     // Base64-encoded data
	MimeType string `json:"mimeType,omitempty"` // MIME type of the content
}

// Standard JSON-RPC 2.0 error codes.
const (
	ErrorCodeParseError     = -32700 // Invalid JSON received
	ErrorCodeInvalidRequest = -32600 // JSON is not a valid request
	ErrorCodeMethodNotFound = -32601 // Method does not exist
	ErrorCodeInvalidParams  = -32602 // Invalid method parameters
	ErrorCodeInternalError  = -32603 // Internal server error
)
