//go:build mage

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// PluginName matches plugin.json#executable. The Grafana plugin SDK loader
// expects backend binaries at `dist/<PluginName>_<os>_<arch>` (flat, with the
// executable-name prefix). Anything else is silently rejected with the error
// `Could not start plugin backend: fork/exec dist/...: no such file or
// directory` — see R2-CR6 in docs/progress/2026-05-14-signing-readiness.md.
const PluginName = "gpx_arc"

// Default target runs when `mage` is invoked with no arguments.
var Default = Build

// Build builds the backend plugin for the current platform.
//
// The output goes to `dist/<PluginName>_<os>_<arch>` (matching Grafana SDK
// loader expectations). Does NOT call Clean — the frontend bundle in `dist/`
// from a previous `npm run build` is preserved. If you want a fully fresh
// rebuild, run `mage clean` explicitly first.
func Build() error {
	fmt.Println("Building backend plugin (current platform)...")
	return buildPlatform(runtime.GOOS, runtime.GOARCH)
}

// BuildAll builds backend binaries for every platform Grafana ships on. Same
// no-Clean behavior as Build (preserves the frontend bundle).
//
// Outputs to `dist/<PluginName>_<os>_<arch>` (and `.exe` on Windows) per
// Grafana SDK loader expectations (R2-CR6).
func BuildAll() error {
	platforms := []struct {
		os   string
		arch string
	}{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"linux", "arm"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
		{"windows", "amd64"},
	}
	for _, platform := range platforms {
		fmt.Printf("Building for %s/%s...\n", platform.os, platform.arch)
		if err := buildPlatform(platform.os, platform.arch); err != nil {
			return err
		}
	}
	return nil
}

// buildPlatform builds the backend binary for one OS/arch combination and
// places it where the Grafana SDK loader expects to find it.
func buildPlatform(goos, goarch string) error {
	binary := fmt.Sprintf("%s_%s_%s", PluginName, goos, goarch)
	if goos == "windows" {
		binary += ".exe"
	}
	if err := os.MkdirAll("dist", 0755); err != nil {
		return err
	}
	outPath := filepath.Join("dist", binary)
	env := map[string]string{"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"}
	return sh.RunWith(env, "go", "build", "-o", outPath, "./pkg")
}

// Clean removes the entire dist/ tree plus any stray root-level binaries.
//
// Run this when you want a fully fresh build — `mage clean && npm run build
// && mage buildAll` is the canonical reset sequence. Build/BuildAll do NOT
// invoke Clean automatically (R2-L15): the previous shape wiped the
// frontend bundle whenever the backend was rebuilt, which interacted badly
// with the webpack `clean: true` output config (both wiping `dist/` left no
// order that produced a complete artifact).
func Clean() error {
	fmt.Println("Cleaning build artifacts...")
	_ = sh.Rm("dist")
	_ = sh.Rm(PluginName)
	_ = sh.Rm(PluginName + ".exe")
	return nil
}

// CleanBackend removes only the backend binaries from dist/, preserving the
// webpack frontend output. Useful inside a single dev iteration where you
// want to force a backend rebuild without re-running `npm run build`.
func CleanBackend() error {
	fmt.Println("Cleaning backend binaries from dist/...")
	matches, err := filepath.Glob(filepath.Join("dist", PluginName+"_*"))
	if err != nil {
		return err
	}
	for _, m := range matches {
		if err := sh.Rm(m); err != nil {
			return err
		}
	}
	_ = sh.Rm(PluginName)
	_ = sh.Rm(PluginName + ".exe")
	return nil
}

// Test runs the Go test suite.
func Test() error {
	fmt.Println("Running tests...")
	return sh.RunV("go", "test", "-v", "./pkg/...")
}

// Fmt formats Go code.
func Fmt() error {
	fmt.Println("Formatting Go code...")
	return sh.RunV("go", "fmt", "./...")
}

// Vet runs go vet against the plugin code.
func Vet() error {
	fmt.Println("Running go vet...")
	return sh.RunV("go", "vet", "./pkg/...")
}

// Dev orchestrates a full development build: frontend bundle first (which
// wipes dist/), then backend binary for the current platform. This is the
// canonical iteration command — `mage dev` produces a complete dist/ tree
// ready for symlinking into a Grafana plugins dir.
//
// Equivalent to `npm run build && mage build` in the correct order. The
// order matters because webpack's `output.clean: true` wipes dist/ on every
// frontend build; backend MUST run after.
func Dev() error {
	mg.SerialDeps(npmBuild)
	return Build()
}

// DevAll orchestrates the release-shape build: frontend first, then every
// platform's backend binary. Equivalent to `npm run build && mage buildAll`.
func DevAll() error {
	mg.SerialDeps(npmBuild)
	return BuildAll()
}

// npmBuild runs the production webpack build. Webpack's `output.clean: true`
// wipes dist/ — see comment in `Dev`.
func npmBuild() error {
	fmt.Println("Building frontend bundle (npm run build)...")
	return sh.RunV("npm", "run", "build")
}
