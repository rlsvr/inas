# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow with Go 1.23, race detection, format check, and `golangci-lint` v2.13.
- `SECURITY.md` vulnerability reporting guidelines.
- Clean badges for Go Reference, CI status, `golangci-lint`, and MIT License.

### Changed
- Lowered Go directive in `go.mod` to Go 1.23 for compatibility with downstream consumers.
