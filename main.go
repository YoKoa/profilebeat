package main

import (
	"os"

	"github.com/YoKoa/profilebeat/cmd"

	_ "github.com/YoKoa/profilebeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
