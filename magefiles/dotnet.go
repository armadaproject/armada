package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var (
	defaultDotnetDockerImg = "mcr.microsoft.com/dotnet/sdk:3.1.417-buster"
	releaseTag             string
	useSystemCerts         bool
)

func initializeDotnetRequirements() {
	releaseTag = getEnvWithDefault("RELEASE_TAG", "UNKNOWN_TAG")
}

func sslCerts() error {
	fmt.Println("Setting up SSL certificates...")
	sslCertsDir := filepath.Join(".", "build", "ssl", "certs")
	err := os.MkdirAll(sslCertsDir, os.ModePerm)
	if err != nil {
		return err
	}

	if _, err := os.Stat("/etc/ssl/certs/ca-certificates.crt"); err == nil {
		err = sh.Run("cp", "/etc/ssl/certs/ca-certificates.crt", filepath.Join(sslCertsDir, "ca-certificates.crt"))
		if err != nil {
			return err
		}
	} else if _, err := os.Stat("/etc/ssl/certs/ca-bundle.crt"); err == nil {
		err = sh.Run("cp", "/etc/ssl/certs/ca-bundle.crt", filepath.Join(sslCertsDir, "ca-certificates.crt"))
		if err != nil {
			return err
		}
	} else if runtime.GOOS == "darwin" {
		err = sh.Run("security", "find-certificate", "-a", "-p", "/System/Library/Keychains/SystemRootCertificates.keychain", ">>", filepath.Join(sslCertsDir, "ca-certificates.crt"))
		if err != nil {
			return err
		}

		err = sh.Run("security", "find-certificate", "-a", "-p", "/Library/Keychains/System.keychain", ">>", filepath.Join(sslCertsDir, "ca-certificates.crt"))
		if err != nil {
			return err
		}

		err = sh.Run("security", "find-certificate", "-a", "-p", "~/Library/Keychains/login.keychain-db", ">>", filepath.Join(sslCertsDir, "ca-certificates.crt"))
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("don't know where to find root CA certs")
	}

	return nil
}

func dotnetSetup() error {
	fmt.Println("Setting up Dotnet...")
	if useSystemCerts {
		err := sslCerts()
		if err != nil {
			return err
		}
	}
	return nil
}

// Target for compiling the dotnet Armada REST client
func Dotnet() error {
	mg.Deps(initializeDotnetRequirements, dotnetSetup, BootstrapProto)
	fmt.Println("Building Dotnet...")

	dotnetCmd := dotnetCmd()

	client := append(dotnetCmd, "dotnet", "build", "./client/DotNet/Armada.Client", "/t:NSwag")
	output, err := dockerOutput(client...)
	fmt.Println(output)
	if err != nil {
		return err
	}

	client = append(dotnetCmd, "dotnet", "build", "./client/DotNet/ArmadaProject.Io.Client")
	output, err = dockerOutput(client...)
	fmt.Println(output)
	if err != nil {
		return err
	}
	return nil
}

// Pack dotnet clients nuget. Requires RELEASE_TAG env var to be set
func PackNuget() error {
	mg.Deps(initializeDotnetRequirements, dotnetSetup, Proto)
	fmt.Println("Pack Nuget...")

	dotnetCmd := dotnetCmd()
	build := append(dotnetCmd, "dotnet", "pack", "client/DotNet/Armada.Client/Armada.Client.csproj", "-c", "Release", "-p:PackageVersion="+releaseTag, "-o", "./bin/client/DotNet")
	output, err := dockerOutput(build...)
	fmt.Println(output)
	if err != nil {
		return err
	}

	build = append(dotnetCmd, "dotnet", "pack", "client/DotNet/ArmadaProject.Io.Client/ArmadaProject.Io.Client.csproj", "-c", "Release", "-p:PackageVersion="+releaseTag, "-o", "./bin/client/DotNet")
	output, err = dockerOutput(build...)
	fmt.Println(output)
	if err != nil {
		return err
	}
	return nil
}
