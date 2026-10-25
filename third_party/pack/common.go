// Copied from go-git v5.19.2's plumbing/format/packfile/common.go. Copyright
// 2018 Sourced Technologies, S.L.; licensed under the Apache License 2.0 in
// LICENSE beside this file.
//
// Changed for Mochi: only the format's constants, which the encoder needs.
//
// SPDX-License-Identifier: Apache-2.0

package pack

var signature = []byte{'P', 'A', 'C', 'K'}

const (
	// VersionSupported is the packfile version supported by this package
	VersionSupported uint32 = 2

	firstLengthBits = uint8(4)   // the first byte into object header has 4 bits to store the length
	lengthBits      = uint8(7)   // subsequent bytes has 7 bits to store the length
	maskFirstLength = 15         // 0000 1111
	maskContinue    = 0x80       // 1000 0000
	maskLength      = uint8(127) // 0111 1111
	maskType        = uint8(112) // 0111 0000
)
