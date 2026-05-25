// Mochi server: App version requirement tests
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"testing"
)

// ============ Version Comparison Tests ============

func TestVersionCompareBasic(t *testing.T) {
	tests := []struct {
		name     string
		v1, v2   string
		expected int
	}{
		// Equal versions
		{"equal simple", "1.0", "1.0", 0},
		{"equal with patch", "1.0.0", "1.0.0", 0},
		{"equal different format", "1.0", "1.0.0", 0},
		{"equal three parts", "1.2.3", "1.2.3", 0},

		// Less than
		{"major less", "0.9", "1.0", -1},
		{"minor less", "1.0", "1.1", -1},
		{"patch less", "1.0.0", "1.0.1", -1},
		{"minor vs patch", "1.0.9", "1.1.0", -1},

		// Greater than
		{"major greater", "2.0", "1.9", 1},
		{"minor greater", "1.2", "1.1", 1},
		{"patch greater", "1.0.2", "1.0.1", 1},

		// Edge cases
		{"zero versions", "0.0", "0.0", 0},
		{"large numbers", "10.20.30", "10.20.29", 1},
		{"double digits", "0.10", "0.9", 1},
		{"triple digits", "1.100", "1.99", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := version_compare(tc.v1, tc.v2)
			if result != tc.expected {
				t.Errorf("version_compare(%q, %q) = %d, expected %d",
					tc.v1, tc.v2, result, tc.expected)
			}
		})
	}
}

func TestVersionCompareSymmetry(t *testing.T) {
	pairs := []struct{ v1, v2 string }{
		{"1.0", "2.0"},
		{"0.1", "0.2"},
		{"1.0.0", "1.0.1"},
		{"0.3", "0.3.1"},
	}

	for _, p := range pairs {
		r1 := version_compare(p.v1, p.v2)
		r2 := version_compare(p.v2, p.v1)

		if r1 == 0 && r2 != 0 {
			t.Errorf("Symmetry broken: %s vs %s", p.v1, p.v2)
		}
		if r1 < 0 && r2 <= 0 {
			t.Errorf("Symmetry broken: %s < %s but reverse not >", p.v1, p.v2)
		}
		if r1 > 0 && r2 >= 0 {
			t.Errorf("Symmetry broken: %s > %s but reverse not <", p.v1, p.v2)
		}
	}
}

func TestVersionCompareTransitivity(t *testing.T) {
	// If a < b and b < c, then a < c
	versions := []string{"0.1", "0.2", "0.3", "1.0", "1.1", "2.0"}

	for i := 0; i < len(versions)-2; i++ {
		a, b, c := versions[i], versions[i+1], versions[i+2]

		ab := version_compare(a, b)
		bc := version_compare(b, c)
		ac := version_compare(a, c)

		if ab < 0 && bc < 0 && ac >= 0 {
			t.Errorf("Transitivity broken: %s < %s < %s but %s >= %s",
				a, b, c, a, c)
		}
	}
}

func TestVersionCompareEmptyStrings(t *testing.T) {
	// Empty strings should be handled gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("version_compare panicked with empty string: %v", r)
		}
	}()

	version_compare("", "1.0")
	version_compare("1.0", "")
	version_compare("", "")
}

func TestVersionCompareMalformed(t *testing.T) {
	// Malformed versions should be handled gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("version_compare panicked with malformed version: %v", r)
		}
	}()

	malformed := []string{
		"abc",
		"1.x",
		"v1.0",
		"1.0.0.0.0",
		"-1.0",
		"1.-1",
		"1.0-beta",
	}

	for _, v := range malformed {
		version_compare(v, "1.0")
		version_compare("1.0", v)
	}
}

// ============ App Version Requirement Tests ============

func TestAppVersionRequirementMinimum(t *testing.T) {
	tests := []struct {
		name          string
		server_version string
		min_required   string
		should_load    bool
	}{
		{"server equals minimum", "0.3.0", "0.3", true},
		{"server above minimum", "0.4.0", "0.3", true},
		{"server below minimum", "0.2.0", "0.3", false},
		{"server way above", "1.0.0", "0.3", true},
		{"server way below", "0.1.0", "0.3", false},
		{"exact match with patch", "0.3.0", "0.3.0", true},
		{"server has extra patch", "0.3.1", "0.3", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meets_requirement := version_compare(tc.server_version, tc.min_required) >= 0
			if meets_requirement != tc.should_load {
				t.Errorf("Server %s with minimum %s: expected load=%v, got %v",
					tc.server_version, tc.min_required, tc.should_load, meets_requirement)
			}
		})
	}
}

func TestAppVersionRequirementMaximum(t *testing.T) {
	tests := []struct {
		name          string
		server_version string
		max_required   string
		should_load    bool
	}{
		{"server equals maximum", "1.0.0", "1.0", true},
		{"server below maximum", "0.9.0", "1.0", true},
		{"server above maximum", "1.1.0", "1.0", false},
		{"server way below", "0.1.0", "1.0", true},
		{"server way above", "2.0.0", "1.0", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meets_requirement := version_compare(tc.server_version, tc.max_required) <= 0
			if meets_requirement != tc.should_load {
				t.Errorf("Server %s with maximum %s: expected load=%v, got %v",
					tc.server_version, tc.max_required, tc.should_load, meets_requirement)
			}
		})
	}
}

func TestAppVersionRequirementRange(t *testing.T) {
	tests := []struct {
		name          string
		server_version string
		min_required   string
		max_required   string
		should_load    bool
	}{
		{"in range", "0.5.0", "0.3", "1.0", true},
		{"at minimum", "0.3.0", "0.3", "1.0", true},
		{"at maximum", "1.0.0", "0.3", "1.0", true},
		{"below range", "0.2.0", "0.3", "1.0", false},
		{"above range", "1.1.0", "0.3", "1.0", false},
		{"tight range", "0.3.5", "0.3", "0.4", true},
		{"same min max", "0.3.0", "0.3", "0.3", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meets_min := version_compare(tc.server_version, tc.min_required) >= 0
			meets_max := version_compare(tc.server_version, tc.max_required) <= 0
			meets_requirement := meets_min && meets_max

			if meets_requirement != tc.should_load {
				t.Errorf("Server %s with range [%s, %s]: expected load=%v, got %v",
					tc.server_version, tc.min_required, tc.max_required,
					tc.should_load, meets_requirement)
			}
		})
	}
}

func TestAppVersionNoRequirement(t *testing.T) {
	// Apps without version requirements should always load
	server_versions := []string{"0.1", "0.3", "1.0", "2.0", "10.0"}

	for _, sv := range server_versions {
		// Empty requirement means no restriction
		min_request := ""
		max_request := ""

		meets_min := min_request == "" || version_compare(sv, min_request) >= 0
		meets_max := max_request == "" || version_compare(sv, max_request) <= 0

		if !meets_min || !meets_max {
			t.Errorf("Server %s should load app with no requirements", sv)
		}
	}
}

// ============ Real-World Version Scenarios ============

func TestMochiVersionScenarios(t *testing.T) {
	// Simulate real Mochi version progression
	scenarios := []struct {
		name         string
		app_min       string
		app_max       string
		compatible   []string
		incompatible []string
	}{
		{
			name:         "repositories app (0.3+)",
			app_min:       "0.3",
			app_max:       "",
			compatible:   []string{"0.3.0", "0.3.1", "0.4.0", "1.0.0"},
			incompatible: []string{"0.2.0", "0.2.37", "0.1.0"},
		},
		{
			name:         "legacy app (0.1-0.2)",
			app_min:       "0.1",
			app_max:       "0.2",
			compatible:   []string{"0.1.0", "0.1.5", "0.2.0", "0.2.37"},
			incompatible: []string{"0.3.0", "1.0.0"},
		},
		{
			name:         "future app (1.0+)",
			app_min:       "1.0",
			app_max:       "",
			compatible:   []string{"1.0.0", "1.1.0", "2.0.0"},
			incompatible: []string{"0.3.0", "0.9.9"},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			for _, v := range sc.compatible {
				meets_min := sc.app_min == "" || version_compare(v, sc.app_min) >= 0
				meets_max := sc.app_max == "" || version_compare(v, sc.app_max) <= 0
				if !meets_min || !meets_max {
					t.Errorf("Version %s should be compatible", v)
				}
			}
			for _, v := range sc.incompatible {
				meets_min := sc.app_min == "" || version_compare(v, sc.app_min) >= 0
				meets_max := sc.app_max == "" || version_compare(v, sc.app_max) <= 0
				if meets_min && meets_max {
					t.Errorf("Version %s should be incompatible", v)
				}
			}
		})
	}
}

// ============ Segment-Based Comparison Tests ============

func TestVersionSegmentPrecision(t *testing.T) {
	// Test that version precision (number of segments) affects comparison
	tests := []struct {
		name     string
		v1, v2   string
		expected int
	}{
		// Two-segment versions treat three-segment as equal if major.minor match
		{"2seg vs 3seg same family", "0.2.37", "0.2", 0},
		{"2seg vs 3seg same family 2", "1.5.99", "1.5", 0},
		{"3seg vs 2seg same family", "0.2", "0.2.37", 0},

		// Three-segment versions compare fully
		{"3seg vs 3seg different", "0.2.37", "0.2.0", 1},
		{"3seg vs 3seg equal", "0.2.0", "0.2.0", 0},
		{"3seg vs 3seg less", "0.2.0", "0.2.37", -1},

		// One-segment versions (major only)
		{"1seg vs 3seg same family", "1.5.3", "1", 0},
		{"1seg vs 2seg same family", "1.5", "1", 0},
		{"1seg vs 1seg greater", "2", "1", 1},
		{"1seg vs 1seg less", "1", "2", -1},
		{"1seg vs 1seg equal", "1", "1", 0},

		// Different families should still compare correctly
		{"2seg different families", "0.3.0", "0.2", 1},
		{"2seg different families 2", "0.2.99", "0.3", -1},
		{"1seg different families", "2.0.0", "1", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := version_compare(tc.v1, tc.v2)
			if result != tc.expected {
				t.Errorf("version_compare(%q, %q) = %d, expected %d",
					tc.v1, tc.v2, result, tc.expected)
			}
		})
	}
}

func TestVersionRequirementExactVsFamily(t *testing.T) {
	// Test the difference between "0.2" (family) and "0.2.0" (exact)
	server_version := "0.2.5"

	// With family requirement (0.2), server 0.2.5 should match
	family_min := "0.2"
	family_max := "0.2"
	meets_family := version_compare(server_version, family_min) >= 0 &&
		version_compare(server_version, family_max) <= 0
	if !meets_family {
		t.Errorf("Server %s should meet family requirement [%s, %s]",
			server_version, family_min, family_max)
	}

	// With exact requirement (0.2.0), server 0.2.5 should NOT match
	exact_max := "0.2.0"
	meets_exact := version_compare(server_version, exact_max) <= 0
	if meets_exact {
		t.Errorf("Server %s should NOT meet exact max %s", server_version, exact_max)
	}
}

func TestVersionRequirementMinimalPrecision(t *testing.T) {
	// Apps can use minimal precision for broad compatibility
	tests := []struct {
		name       string
		server     string
		min_request     string
		should_pass bool
	}{
		// "1" means any 1.x.x version
		{"1.0.0 meets min 1", "1.0.0", "1", true},
		{"1.5.3 meets min 1", "1.5.3", "1", true},
		{"1.99.99 meets min 1", "1.99.99", "1", true},
		{"0.9.9 fails min 1", "0.9.9", "1", false},
		{"2.0.0 meets min 1", "2.0.0", "1", true},

		// "0.3" means any 0.3.x version
		{"0.3.0 meets min 0.3", "0.3.0", "0.3", true},
		{"0.3.99 meets min 0.3", "0.3.99", "0.3", true},
		{"0.2.99 fails min 0.3", "0.2.99", "0.3", false},
		{"0.4.0 meets min 0.3", "0.4.0", "0.3", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meets_min := version_compare(tc.server, tc.min_request) >= 0
			if meets_min != tc.should_pass {
				t.Errorf("Server %s with min %s: expected %v, got %v",
					tc.server, tc.min_request, tc.should_pass, meets_min)
			}
		})
	}
}

func TestVersionRequirementRangeWithDifferentPrecision(t *testing.T) {
	tests := []struct {
		name       string
		server     string
		min_request     string
		max_request     string
		should_pass bool
	}{
		// Range [0.2, 0.3] should include all 0.2.x and 0.3.x
		{"0.2.0 in [0.2, 0.3]", "0.2.0", "0.2", "0.3", true},
		{"0.2.99 in [0.2, 0.3]", "0.2.99", "0.2", "0.3", true},
		{"0.3.0 in [0.2, 0.3]", "0.3.0", "0.2", "0.3", true},
		{"0.3.99 in [0.2, 0.3]", "0.3.99", "0.2", "0.3", true},
		{"0.1.99 not in [0.2, 0.3]", "0.1.99", "0.2", "0.3", false},
		{"0.4.0 not in [0.2, 0.3]", "0.4.0", "0.2", "0.3", false},

		// Range [0.2.0, 0.3.0] is exact
		{"0.2.0 in [0.2.0, 0.3.0]", "0.2.0", "0.2.0", "0.3.0", true},
		{"0.2.5 in [0.2.0, 0.3.0]", "0.2.5", "0.2.0", "0.3.0", true},
		{"0.3.0 in [0.2.0, 0.3.0]", "0.3.0", "0.2.0", "0.3.0", true},
		{"0.3.1 not in [0.2.0, 0.3.0]", "0.3.1", "0.2.0", "0.3.0", false},

		// Mixed precision: min exact, max family
		{"0.2.0 in [0.2.0, 0.3]", "0.2.0", "0.2.0", "0.3", true},
		{"0.1.99 not in [0.2.0, 0.3]", "0.1.99", "0.2.0", "0.3", false},
		{"0.3.99 in [0.2.0, 0.3]", "0.3.99", "0.2.0", "0.3", true},
		{"0.4.0 not in [0.2.0, 0.3]", "0.4.0", "0.2.0", "0.3", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meets_min := version_compare(tc.server, tc.min_request) >= 0
			meets_max := version_compare(tc.server, tc.max_request) <= 0
			meets_range := meets_min && meets_max

			if meets_range != tc.should_pass {
				t.Errorf("Server %s in [%s, %s]: expected %v, got %v",
					tc.server, tc.min_request, tc.max_request, tc.should_pass, meets_range)
			}
		})
	}
}
