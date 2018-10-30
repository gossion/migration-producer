package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gossion/migration-producer/cmd/subcommands"
	flags "github.com/jessevdk/go-flags"
)

type MigratorCommand struct {
	Migrate subcommands.DBMigrateCommand `command:"migrate-db" description:"Migrate database from one database to another"`
}

var Migrator MigratorCommand

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	parser := flags.NewParser(&Migrator, flags.HelpFlag)
	parser.NamespaceDelimiter = "-"

	_, err := parser.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
