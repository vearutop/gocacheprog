// Command gocacheprog is a remote Go build/test cache purpose-built for CI. All actual logic
// lives in the importable cli package (see cli.Main), so a third-party binary can embed or
// soft-fork gocacheprog by calling cli.Main directly instead of forking this whole module.
package main

import (
	"fmt"
	"os"

	"github.com/vearutop/gocacheprog/cli"
)

func main() {
	if err := cli.Main(); err != nil {
		// Bypasses the log package deliberately: -quiet redirects it to io.Discard, and a
		// fatal exit must always be visible regardless of that.
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
