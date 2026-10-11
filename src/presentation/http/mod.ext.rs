// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// Hand-authored guarded composition (read-only CRUD + validated verbs). User-owned.
pub mod guarded_routes;
