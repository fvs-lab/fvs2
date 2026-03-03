module fvs2

go 1.24.0

replace fvs-v2-core => ../core

require (
	fvs-v2-core v0.0.0-00010101000000-000000000000
	github.com/mirkobrombin/go-cli-builder/v2 v2.0.5
	github.com/zeebo/blake3 v0.2.4
)

require (
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/mirkobrombin/go-foundation v0.2.0 // indirect
)
