package database

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver for database/sql
	"github.com/gossion/migration-producer/pkg/utils"
)

func init() {
	RegisterDriver(MySQLDriver{}, "mysql")
}

// MySQLDriver provides top level database functions
type MySQLDriver struct {
}

// check if mysql, mysqldump exist in env,
// check if the version is compatible
func (drv MySQLDriver) CheckDependency() error {
	cmds := []string{"mysql", "mysqldump"}
	log.Printf("Checking cmd dependency: %s", cmds)

	for _, cmd := range cmds {
		path, err := exec.LookPath(cmd)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("Found %s at %s", cmd, path)
	}
	return nil
}

// check the database is valid
// Use scenario: should check if the URL is valid before migration
func (drv MySQLDriver) IsValid(u *url.URL) error {
	return nil
}

// Ping verifies a connection to the database server. It does not verify whether the
// specified database exists.
func (drv MySQLDriver) Ping(u *url.URL) error {
	db, err := drv.openRootDB(u)
	if err != nil {
		return err
	}
	defer mustClose(db)

	return db.Ping()
}

// Open creates a new database connection
func (drv MySQLDriver) Open(u *url.URL) (*sql.DB, error) {
	return sql.Open("mysql", normalizeMySQLURL(u))
}

// Dump the current database
func (drv MySQLDriver) Export(u *url.URL) (string, error) {
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		log.Println(err)
		return "", err
	}
	defer tmpfile.Close()

	log.Printf("Will export mysql db to file: %s", tmpfile.Name())

	args := mysqldumpArgs(u)
	output, err := utils.RunCommandOutTOFile("mysqldump", tmpfile, args...)
	if err != nil {
		return "", err
	}

	f, err := os.Stat(tmpfile.Name())
	if err != nil {
		log.Printf("Error stat of exported file: %s", err)
		return "", err
	}
	if f.Size() == 0 {
		log.Printf("Nothing was exported to file: %s", tmpfile.Name())
		return "", errors.New("Nothing exported")
	}

	log.Printf("mysqldump output: %s", output)
	return tmpfile.Name(), nil
}

func (drv MySQLDriver) Import(u *url.URL, filename string) error {
	err := drv.CreateDbIfNotExists(u)
	if err != nil {
		log.Println(err)
		return err
	}

	f, err := os.Open(filename)
	if err != nil {
		log.Println("Failed to open file", filename)
		return err
	}
	args := mysqlArgs(u)
	_, err = utils.RunCommandWithStdin("mysql", f, args...)
	if err != nil {
		return err
	}

	return nil
}

func (drv MySQLDriver) Lock(u *url.URL) error {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return err
	}

	rows, err := db.Exec("FLUSH TABLES WITH READ LOCK")
	if err != nil {
		log.Printf("Failed to lock db %s", name)
		return err
	}
	log.Println("LOCK DATABASE:", rows) //TODO: rm

	return nil
}

func (drv MySQLDriver) UnLock(u *url.URL) error {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return err
	}

	rows, err := db.Exec("UNLOCK TABLES")
	if err != nil {
		log.Printf("Failed to unlock db %s", name)
		return err
	}
	log.Println("UNLOCK DATABASE:", rows) //TODO: rm

	return nil
}

func (drv MySQLDriver) GetSum(u *url.URL) (map[string]int, error) {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return nil, err
	}

	//For InnoDB tables, the row count is only a rough estimate used in SQL optimization. You'll need to use COUNT(*) for exact counts (which is more expensive)
	rows, err := db.Query("SELECT table_name, TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '?'", name)
	if err != nil {
		log.Printf("Failed to query db %s for all table count", name)
		return nil, err
	}
	log.Println(rows) //TODO: parse, scan

	return make(map[string]int), nil
}

// Create database if it is not exist
func (drv MySQLDriver) CreateDbIfNotExists(u *url.URL) error {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return err
	}

	rows, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + name)
	if err != nil {
		log.Printf("Failed to create db %s", name)
		return err
	}
	log.Println("CREATE DATABASE:", rows) //TODO: rm
	return nil
}

// DatabaseExists determines whether the database exists
func (drv MySQLDriver) DatabaseExists(u *url.URL) (bool, error) {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		return false, err
	}
	defer mustClose(db)

	exists := false
	err = db.QueryRow("select true from information_schema.schemata "+
		"where schema_name = ?", name).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}

	return exists, err
}

// helpers

// mustClose ensures a stream is closed
func mustClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}

func normalizeMySQLURL(u *url.URL) string {
	normalizedURL := *u
	normalizedURL.Scheme = ""

	// set default port
	if normalizedURL.Port() == "" {
		normalizedURL.Host = fmt.Sprintf("%s:3306", normalizedURL.Host)
	}

	// host format required by go-sql-driver/mysql
	normalizedURL.Host = fmt.Sprintf("tcp(%s)", normalizedURL.Host)

	query := normalizedURL.Query()
	query.Set("multiStatements", "true")
	normalizedURL.RawQuery = query.Encode()

	str := normalizedURL.String()
	return strings.TrimLeft(str, "/")
}

func mysqlArgs(u *url.URL) []string {
	args := []string{}

	if hostname := u.Hostname(); hostname != "" {
		args = append(args, "--host="+hostname)
	}
	if port := u.Port(); port != "" {
		args = append(args, "--port="+port)
	}
	if username := u.User.Username(); username != "" {
		args = append(args, "--user="+username)
	}
	// mysql recommends against using environment variables to supply password
	// https://dev.mysql.com/doc/refman/5.7/en/password-security-user.html
	if password, set := u.User.Password(); set {
		args = append(args, "--password="+password)
	}
	// add database name
	args = append(args, strings.TrimLeft(u.Path, "/"))

	return args
}

func mysqldumpArgs(u *url.URL) []string {
	// generate CLI arguments
	args := []string{"--opt", "--routines"}
	//"--no-data", "--skip-dump-date", "--skip-add-drop-table"}

	args = append(args, mysqlArgs(u)...)

	return args
}

// databaseName returns the database name from a URL
func databaseName(u *url.URL) string {
	name := u.Path
	if len(name) > 0 && name[:1] == "/" {
		name = name[1:]
	}

	return name
}

func (drv MySQLDriver) openRootDB(u *url.URL) (*sql.DB, error) {
	// connect to no particular database
	rootURL := *u
	rootURL.Path = "/"

	return drv.Open(&rootURL)
}
