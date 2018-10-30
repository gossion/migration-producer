package subcommands

import (
	"github.com/gossion/migration-producer/pkg/datatype"
	"github.com/gossion/migration-producer/pkg/migrator"
)

type DBMigrateCommand struct {
	SourceDSN      string `long:"source-dsn" env:"SOURCE_DSN"`
	DestinationDSN string `long:"dest-dsn" env:"DEST_DSN"`
}

func (c *DBMigrateCommand) Execute([]string) error {

	//src, err := datatype.ParseDSN(c.SourceDSN)
	//dest, err := datatype.ParseDSN(c.DestinationDSN)

	// test
	src := datatype.Database{
		Username: "g2",
		Password: "g2",
		Protocal: "mysql",
		Host:     "127.0.0.1",
		Database: "ops",
	}

	dest := datatype.Database{
		Username: "g2",
		Password: "g2",
		Protocal: "mysql",
		Host:     "127.0.0.1",
		Database: "ops2",
	}

	dm := migrator.NewDatabaseMigrator(src, dest)
	err := dm.Migrate()
	if err != nil {
		return err
	}
	return nil
}
