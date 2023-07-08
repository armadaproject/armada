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
	nugetApiKey            string
	useSystemCerts         bool
)

func initializeDotnetRequirements() {
	releaseTag = getEnvWithDefault("RELEASE_TAG", "UNKNOWN_TAG")
	nugetApiKey = getEnvWithDefault("NUGET_API_KEY", "UNKNOWN_NUGET_API_KEY")
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

// Pack and push dotnet clients to nuget. Requires RELEASE_TAG and NUGET_API_KEY env vars to be set
func PushNuget() error {
	mg.Deps(initializeDotnetRequirements, dotnetSetup, Proto)
	fmt.Println("Pushing to Nuget...")

	dotnetCmd := dotnetCmd()
	push := append(dotnetCmd, "dotnet", "pack", "client/DotNet/Armada.Client/Armada.Client.csproj", "-c", "Release", "-p:PackageVersion="+releaseTag, "-o", "./bin/client/DotNet")
	output, err := dockerOutput(push...)
	fmt.Println(output)
	if err != nil {
		return err
	}

	push = append(dotnetCmd, "dotnet", "nuget", "push", "./bin/client/DotNet/G-Research.Armada.Client."+releaseTag+".nupkg", "-k", nugetApiKey, "-s", "https://api.nuget.org/v3/index.json")
	output, err = dockerOutput(push...)
	fmt.Println(output)
	if err != nil {
		return err
	}

	push = append(dotnetCmd, "dotnet", "pack", "client/DotNet/ArmadaProject.Io.Client/ArmadaProject.Io.Client.csproj", "-c", "Release", "-p:PackageVersion="+releaseTag, "-o", "./bin/client/DotNet")
	output, err = dockerOutput(push...)
	fmt.Println(output)
	if err != nil {
		return err
	}

	push = append(dotnetCmd, "dotnet", "nuget", "push", "./bin/client/DotNet/ArmadaProject.Io.Client."+releaseTag+".nupkg", "-k", nugetApiKey, "-s", "https://api.nuget.org/v3/index.json")
	output, err = dockerOutput(push...)
	fmt.Println(output)
	if err != nil {
		return err
	}
	return nil
}
