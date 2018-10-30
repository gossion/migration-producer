package database

import (
	"database/sql"
	"fmt"
	"net/url"
)

type DatabaseDriver interface {
	CheckDependency() error
	IsValid(u *url.URL) error
	Ping(u *url.URL) error
	Open(*url.URL) (*sql.DB, error)
	Export(*url.URL) (string, error) //return sql file, error
	Import(*url.URL, string) error   //return output?
	Lock(u *url.URL) error
	UnLock(u *url.URL) error
	//read all tables
	GetSum(*url.URL) (map[string]int, error)
}

var drivers = map[string]DatabaseDriver{}

//Register driver
func RegisterDriver(drv DatabaseDriver, scheme string) {
	drivers[scheme] = drv
}

// GetDriver loads a database driver by name
func GetDriver(name string) (DatabaseDriver, error) {
	if val, ok := drivers[name]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("unsupported driver: %s", name)
}
