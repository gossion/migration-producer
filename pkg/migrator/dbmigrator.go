package migrator

import (
	"errors"
	"fmt"
	"log"

	"github.com/gossion/migration-producer/pkg/database"
	"github.com/gossion/migration-producer/pkg/datatype"
)

const FullDump = "fulldump"

type DatabaseMigrator struct {
	Method        string
	LockBeforeOps bool
	Source        datatype.Database
	Destination   datatype.Database
}

func NewDatabaseMigrator(src datatype.Database, dest datatype.Database) DatabaseMigrator {
	return DatabaseMigrator{
		Method:        FullDump,
		LockBeforeOps: false,
		Source:        src,
		Destination:   dest,
	}
}

func (dm DatabaseMigrator) Migrate() error {
	err := dm.CheckCompatibility()
	if err != nil {
		return err
	}

	err = dm.CheckConnections()
	if err != nil {
		return err
	}

	drv, err := database.GetDriver(dm.Source.Protocal)
	if err != nil {
		return err
	}

	err = drv.CheckDependency()
	if err != nil {
		return err
	}

	if dm.LockBeforeOps {
		//lock
		//defer unlock
		// or put lock as param of Export

		//get summary, which should be compared with dest
	}

	//export
	src_url, _ := dm.Source.ToURL() //error already checked by CheckConnections
	fn, err := drv.Export(src_url)
	if err != nil {
		return err
	}
	fmt.Println(fn)

	// import
	dest_url, _ := dm.Destination.ToURL()
	err = drv.Import(dest_url, fn)
	if err != nil {
		return err
	}
	//validate

	return nil
}

// check if the source and dest has compatible schema,version
func (dm DatabaseMigrator) CheckCompatibility() error {
	if dm.Source.Protocal != dm.Destination.Protocal {
		return errors.New("Not compatiable protocal")
	}
	//TODO: version
	return nil
}

func (dm DatabaseMigrator) CheckConnections() error {
	drv, err := database.GetDriver(dm.Source.Protocal)
	if err != nil {
		log.Println("Failed to get driver for", dm.Source.Protocal)
		return err
	}

	src_url, err := dm.Source.ToURL()
	err = drv.Ping(src_url)
	if err != nil {
		log.Println("Failed to Ping", src_url)
		return err
	}

	dest_url, err := dm.Destination.ToURL()
	err = drv.Ping(dest_url)
	if err != nil {
		log.Println("Failed to Ping", dest_url)
		return err
	}

	return nil
}
