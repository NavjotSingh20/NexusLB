# 01 : Compile-Time Encapsulation Failure in Multi-Package go:embed Asset Bundling

# Boundary Violation in Static Asset Embedding

## Error:

Fatal compilation error when attempting to embed static dashboard assets across package directory boundaries:

> # nexuslb/pkg/dashboard
> pkg\dashboard\server.go:16:13: pattern ../../web/*: cannot use '..' in embed path

## Relevant Context

NexusLB uses a modular architecture where the monitoring dashboard HTTP server is maintained in `pkg/dashboard/`, while the frontend web assets (`index.html`, `style.css`, `app.js`) reside in the root `web/` directory.

To ensure NexusLB ships as a self-contained, standalone single binary without external file path runtime dependencies, `pkg/dashboard/server.go` attempted to embed the UI files directly:

```go
package dashboard

import (
    "embed"
    "net/http"
)

//go:embed ../../web/*
var webAssets embed.FS

func (s *Server) Start() error {
    fileServer := http.FileServer(http.FS(webAssets))
    // ...
}
```

When building the project with `go build ./...`, the compiler rejected the directive.

## Key Observation

The Go compiler's `embed` package design specification (introduced in Go 1.16) strictly prohibits relative path traversal involving `..` segments (`pattern ../../web/*: cannot use '..' in embed path`).

This design decision is enforced intentionally by the Go toolchain to maintain strong package encapsulation:
1. A package cannot reach outside its own directory tree to embed files from a parent or sibling directory.
2. Embedding paths that escape the package root would violate module hermeticity and break repeatable package caching across different build environments.

Attempting to bypass this by reading assets from disk with `os.ReadFile` or `http.Dir("web")` would undermine the single-binary deployment goal, requiring users to manually run the executable strictly from the root working directory or risk HTTP 404 file-not-found crashes in production deployments.

## Solution

Decoupled the embedded asset declaration into a dedicated package `web` located directly within the `web/` directory ([`web/web.go`](file:///c:/Users/navjo/OneDrive/Desktop/NexusLB/web/web.go)), exporting the embedded filesystem as a public package variable:

```go
// In web/web.go (co-located with HTML, CSS, JS)
package web

import "embed"

//go:embed index.html style.css app.js
var Assets embed.FS
```

Then updated `pkg/dashboard/server.go` to cleanly import `nexuslb/web` and mount the exported `io/fs.FS`:

```go
package dashboard

import (
    "net/http"
    "nexuslb/web"
)

func (s *Server) Start() error {
    mux := http.NewServeMux()
    fileServer := http.FileServer(http.FS(web.Assets))
    mux.Handle("/", fileServer)
    // ...
}
```

**Because**

Placing `//go:embed` inside a package co-located with the target assets satisfies the Go compiler's directory boundary constraints without using invalid `..` relative traversals. Exposing `Assets` as an `embed.FS` allows downstream consumers (like `pkg/dashboard`) to consume the bundled virtual filesystem as an immutable `io/fs.FS` abstraction, preserving both package isolation and zero-dependency single-binary compilation.
