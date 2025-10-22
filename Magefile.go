// +build mage

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Default target to run when none is specified
var Default = Build

// Build builds the backend plugin binary
func Build() error {
	mg.Deps(Clean)
	fmt.Println("Building backend plugin...")

	return buildBinary("gpx_arc")
}

// BuildAll builds binaries for all supported platforms
func BuildAll() error {
	mg.Deps(Clean)

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

		binary := "gpx_arc"
		if platform.os == "windows" {
			binary = "gpx_arc.exe"
		}

		env := map[string]string{
			"GOOS":   platform.os,
			"GOARCH": platform.arch,
		}

		outPath := filepath.Join("dist", fmt.Sprintf("%s_%s", platform.os, platform.arch), binary)

		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return err
		}

		if err := sh.RunWith(env, "go", "build", "-o", outPath, "./pkg"); err != nil {
			return err
		}
	}

	return nil
}

// Clean removes build artifacts
func Clean() error {
	fmt.Println("Cleaning build artifacts...")
	_ = sh.Rm("dist")
	_ = sh.Rm("gpx_arc")
	_ = sh.Rm("gpx_arc.exe")
	return nil
}

// Test runs Go tests
func Test() error {
	fmt.Println("Running tests...")
	return sh.RunV("go", "test", "-v", "./...")
}

// Fmt formats Go code
func Fmt() error {
	fmt.Println("Formatting Go code...")
	return sh.RunV("go", "fmt", "./...")
}

// buildBinary builds a single binary for the current platform
func buildBinary(name string) error {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}

	return sh.RunV("go", "build", "-o", name, "./pkg")
}
