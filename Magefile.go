//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"golang.org/x/sync/errgroup"
)

// pluginName resolves the backend executable name from plugin.json's
// `executable` field — single source of truth. The Grafana plugin SDK
// loader expects backend binaries at `dist/<executable>_<os>_<arch>`
// (flat, with the executable-name prefix). Anything else is silently
// rejected with `Could not start plugin backend: fork/exec dist/...:
// no such file or directory` — see R2-CR6 in
// docs/progress/2026-05-14-signing-readiness.md.
//
// Reading dynamically avoids the drift hazard from a hardcoded const
// whose plugin.json counterpart could be edited independently. Falls
// back to "gpx_arc" only if plugin.json is missing/malformed so a
// stripped-down checkout (or an early bootstrap) still produces a
// recognizable filename.
//
// Memoized via sync.Once: a single `BuildAll` invocation calls this 6
// times (once per platform) plus more from Clean/CleanBackend.
// Repeated filesystem reads and duplicate stderr warnings on missing
// plugin.json are wasted work — sync.Once also makes it safe under
// Mage's parallel target execution.
var (
	pluginNameOnce  sync.Once
	pluginNameValue string
)

func pluginName() string {
	pluginNameOnce.Do(func() {
		const fallback = "gpx_arc"
		data, err := os.ReadFile("plugin.json")
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: could not read plugin.json (%v); using fallback executable name %q\n", err, fallback)
			pluginNameValue = fallback
			return
		}
		var meta struct {
			Executable string `json:"executable"`
		}
		if err := json.Unmarshal(data, &meta); err != nil {
			// Surface the specific parse error — without it, a syntax
			// error in plugin.json is invisible to whoever runs the build.
			fmt.Fprintf(os.Stderr, "warn: could not parse plugin.json (%v); using fallback executable name %q\n", err, fallback)
			pluginNameValue = fallback
			return
		}
		if meta.Executable == "" {
			fmt.Fprintf(os.Stderr, "warn: plugin.json#executable is empty; using fallback %q\n", fallback)
			pluginNameValue = fallback
			return
		}
		pluginNameValue = meta.Executable
	})
	return pluginNameValue
}

// Default target runs when `mage` is invoked with no arguments.
var Default = Build

// Build builds the backend plugin for the current platform.
//
// The output goes to `dist/<plugin.json#executable>_<os>_<arch>` (matching Grafana SDK
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
// Outputs to `dist/<plugin.json#executable>_<os>_<arch>` (and `.exe` on Windows)
// per Grafana SDK loader expectations (R2-CR6). The platform list is shared
// with CleanBackend via buildPlatforms() so a new target gets cleaned too.
//
// Builds run in parallel — go's cross-compile is CPU-bound and independent
// per target, so 6 concurrent `go build` invocations shave ~70% off a
// serial run on a multi-core machine.
//
// Note: `mg.Deps` cannot be used here. Mage memoizes by the function
// pointer of each closure, and all closures defined in the same loop
// share the same compiled function (`main.BuildAll.func1`) → Mage treats
// them as one dep and runs only the first iteration's body. We use
// `errgroup` directly to get real parallelism with correct error
// propagation.
func BuildAll() error {
	g := new(errgroup.Group)
	for _, p := range buildPlatforms() {
		p := p
		g.Go(func() error {
			fmt.Printf("Building for %s/%s...\n", p.os, p.arch)
			return buildPlatform(p.os, p.arch)
		})
	}
	return g.Wait()
}

// buildPlatform builds the backend binary for one OS/arch combination and
// places it where the Grafana SDK loader expects to find it.
func buildPlatform(goos, goarch string) error {
	if err := os.MkdirAll("dist", 0755); err != nil {
		return err
	}
	outPath := filepath.Join("dist", platformBinary(goos, goarch))
	env := map[string]string{"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"}
	// Pin GOARM=7 on linux/arm so the binary actually runs on the hardware
	// users have. Go's default GOARM=6 targets ARMv6 (Raspberry Pi 1 / Zero
	// W) but lacks hardware float, which trips Pi 3/4/5 and most modern ARM
	// SBCs. GOARM=7 matches what the Grafana official plugins ship.
	if goarch == "arm" {
		env["GOARM"] = "7"
	}
	// -trimpath strips the local build-machine path from the binary
	//   (reproducibility, no leaking /Users/nacho/... into a signed plugin).
	// -ldflags="-s -w" strips symbol tables and debug info
	//   (~30% smaller binary; Grafana plugins ship cross-compiled, so
	//    binary size matters in the signed bundle).
	// Both are baseline recommendations from the Grafana plugin signing
	// docs and the grafana-plugin-sdk-go README.
	return sh.RunWith(env, "go", "build",
		"-trimpath",
		"-ldflags", "-s -w",
		"-o", outPath, "./pkg")
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
	_ = sh.Rm(pluginName())
	_ = sh.Rm(pluginName() + ".exe")
	return nil
}

// CleanBackend removes only the backend binaries — both the per-platform
// outputs in `dist/<exe>_<os>_<arch>` and any stray root-level `<exe>` /
// `<exe>.exe` from older `go build` invocations — while preserving the
// webpack frontend output in dist/. Useful inside a single dev iteration
// where you want to force a backend rebuild without re-running
// `npm run build`.
//
// Uses the same buildPlatforms() set as BuildAll rather than a `<exe>_*`
// glob (which could in principle match non-binary files sharing the
// prefix). The known-platforms list is the only set BuildAll ever produces.
func CleanBackend() error {
	fmt.Println("Cleaning backend binaries...")
	for _, p := range buildPlatforms() {
		_ = sh.Rm(filepath.Join("dist", platformBinary(p.os, p.arch)))
	}
	// Legacy: `mage build` (single-platform target) used to drop the binary
	// at repo root. New shape puts it under dist/, but clean up any stragglers
	// from older trees.
	_ = sh.Rm(pluginName())
	_ = sh.Rm(pluginName() + ".exe")
	return nil
}

// platform describes one cross-compile target.
type platform struct {
	os   string
	arch string
}

// buildPlatforms returns the canonical list of (os, arch) targets we build
// release binaries for. Used by BuildAll and CleanBackend so the two stay
// in lock-step — adding a platform to BuildAll automatically extends
// CleanBackend's cleanup scope.
func buildPlatforms() []platform {
	return []platform{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"linux", "arm"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
		{"windows", "amd64"},
	}
}

// platformBinary returns the filename for one platform's backend binary,
// per Grafana SDK loader expectations (R2-CR6).
func platformBinary(goos, goarch string) string {
	name := fmt.Sprintf("%s_%s_%s", pluginName(), goos, goarch)
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

// Test runs the Go test suite across the whole module (./...) so any
// future packages — internal/, cmd/, top-level helpers — are covered
// without a Magefile edit. Matches `Fmt`'s scope.
func Test() error {
	fmt.Println("Running tests...")
	return sh.RunV("go", "test", "-v", "./...")
}

// Fmt formats Go code.
func Fmt() error {
	fmt.Println("Formatting Go code...")
	return sh.RunV("go", "fmt", "./...")
}

// Vet runs go vet across the whole module (./...). Same rationale as
// Test — catches issues in any package, not just pkg/.
func Vet() error {
	fmt.Println("Running go vet...")
	return sh.RunV("go", "vet", "./...")
}

// Dev orchestrates a full development build: frontend bundle first (which
// wipes dist/), then backend binary for the current platform. This is the
// canonical iteration command — `mage dev` produces a complete dist/ tree
// ready for symlinking into a Grafana plugins dir.
//
// The order matters because webpack's `output.clean: true` wipes dist/ on
// every frontend build; backend MUST run after. `mg.SerialDeps` enforces
// strict ordering AND lets Mage memoize each step so repeated `Dev`
// invocations don't re-run completed targets.
func Dev() error {
	mg.SerialDeps(npmBuild, Build)
	return nil
}

// DevAll orchestrates the release-shape build: frontend first, then every
// platform's backend binary. Same SerialDeps pattern as Dev.
func DevAll() error {
	mg.SerialDeps(npmBuild, BuildAll)
	return nil
}

// npmBuild runs the production webpack build. Webpack's `output.clean: true`
// wipes dist/ — see comment in `Dev`.
func npmBuild() error {
	fmt.Println("Building frontend bundle (npm run build)...")
	return sh.RunV("npm", "run", "build")
}
