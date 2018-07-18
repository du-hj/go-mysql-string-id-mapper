package idmapper

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"sync"
)

var (
	db             *sql.DB
	tablePrefix    string
	idMappers      map[string]*IdMapper
	idMappersMutex *sync.RWMutex
)

func newIdMapper(mapperName string) *IdMapper {
	return &IdMapper{
		tableName: mapperTableName(mapperName),
		s2id:      make(map[string]uint32),
		id2s:      make([]string, 0, 0),
		mutex:     &sync.RWMutex{},
	}
}

func (mapper *IdMapper) loadFromTable() error {
	size := -1
	query := `SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ? AND COLUMN_NAME = ?`
	if err := db.QueryRow(query, mapper.tableName, "name").Scan(&size); err != nil {
		if err == sql.ErrNoRows {
			return ErrorNotFound
		} else {
			log.Fatal("[idmapper] Query error: %+v", err)
		}
	}

	mapper.maxKeySize = size

	mapper.mutex.Lock()
	defer mapper.mutex.Unlock()

	nrows := -1
	query = "select max(idx) from `" + mapper.tableName + "`"
	rows, e2 := db.Query(query)
	if e2 != nil {
		return e2
	}
	if rows.Next() {
		rows.Scan(&nrows)
	} else {
		return fmt.Errorf("Can not get the number of rows")
	}
	rows.Close()

	// ----

	alreadyMapped := make(map[string]int)
	s2id := make(map[string]uint32)
	id2s := make([]string, nrows+1)
	idxsync := make(chan int64)
	alldone := sync.WaitGroup{}
	alldone.Add(1)

	// ----

	mapperFun := func() {
		cidx := int64(1)
		for idx := range idxsync {
			if idx == 0 {
				break
			}
			for cidx <= idx {
				name := id2s[cidx]
				if _, has := s2id[name]; has {
					if _, has := alreadyMapped[name]; !has {
						alreadyMapped[name] = 1
					}
					alreadyMapped[name] += 1
				}
				s2id[name] = uint32(cidx)
				cidx += 1
			}
		}
		alldone.Done()
	}

	// ----

	readerFun := func() error {
		query := "select idx,name from `" + mapper.tableName + "`"
		stmt, err := db.Prepare(query)
		if err != nil {
			return err
		}

		rows, err := stmt.Query()
		if err != nil {
			stmt.Close()
			return err
		}

		count := 0
		total := 0
		var idx int64
		var iname string
		// start := time.Now()
		for rows.Next() {
			rows.Scan(&idx, &iname)
			id2s[idx] = iname
			count += 1
			if count >= 100*1000 {
				total += count
				idxsync <- idx
				count = 0

				// dt := time.Since(start)
				// s.log("item:"+s.table, "%s items read in %v [%dK read/sec]",
				//  utils.Numerize(total), dt, total*int(time.Millisecond)/int(dt))

			}
		}

		rows.Close()
		stmt.Close()

		if idx != 0 {
			idxsync <- idx
		}

		return nil
	}

	go mapperFun()
	var gError error
	go func() {
		gError = readerFun()
		idxsync <- 0
	}()

	alldone.Wait()

	if gError != nil {
		return gError
	}

	if len(alreadyMapped) > 0 {
		log.Printf(mapper.tableName, "there are %d duplicated items...", len(alreadyMapped))
		// for name, count := range alreadyMapped {
		//  s.loge("item:"+s.table, "item '%s' is mapped %d times!", name, count)
		// }
	}

	mapper.s2id = s2id
	mapper.id2s = id2s

	return nil
}

var supportUCS2 = true

func (mapper *IdMapper) createTable() error {
	/* Since some DB do not support UCS2, we must try all
	 * character set one-by-one. Once a character set is
	 * failed, it needs to be remembered so that next
	 * time, the same character set is not retried.
	 */
	var err error
	const UCS2 string = "ucs2"
	for _, charset := range []string{UCS2, "utf8"} {

		switch charset {
		case UCS2:
			mapper.maxKeySize = 500
			if !supportUCS2 {
				continue
			}
		case "utf8":
			mapper.maxKeySize = 333
		default:
			panic("Unknown character set " + charset)
		}

		cquery := "create table IF NOT EXISTS `" + mapper.tableName + "` (" +
			" `idx` int unsigned NOT NULL AUTO_INCREMENT," +
			" `name` VARCHAR(" + fmt.Sprint(mapper.maxKeySize) + ") CHARACTER SET " + charset + ", " +
			"  PRIMARY KEY (`idx`), " +
			"  UNIQUE KEY `name` (`name`) " +
			") ENGINE=MyISAM  DEFAULT CHARSET=" + charset + ";"

		_, err = db.Exec(cquery)
		if err == nil {
			break
		}
		if merr, ok := err.(*mysql.MySQLError); ok {
			if merr.Number == 1115 /*Unknown character set*/ {
				if charset == UCS2 {
					supportUCS2 = false
					log.Printf(mapper.tableName, "Can not use UCS2... trying UTF-8")
				}
				continue
			}
		}

	}

	return err
}

func (mapper *IdMapper) insertItem(item string) (uint32, error) {
	var idx uint32
	if result, err := db.Exec("insert into `"+mapper.tableName+"` values (?,?)", 0, item); err != nil {
		if merr, ok := err.(*mysql.MySQLError); ok {
			if merr.Number == 1062 {
				//  Duplicate entry ... This can happen when multiple instance of a server
				// are running concurently
				query := "select idx from `" + mapper.tableName + "` where name=?"
				err = db.QueryRow(query, item).Scan(&idx)

				log.Printf(mapper.tableName, "Duplicate item '%s'", item)

			} else if merr.Number == 1366 {
				// Error 1366: Incorrect string value: '\xEF\xBF' for column 'name' at row 1
				log.Printf(mapper.tableName, "Invalid item '%s' -> can not insert", item)
				return INVALID, nil
			}

		}
		return idx, err

	} else {
		id, err := result.LastInsertId()
		return uint32(id), err
	}
}

func init() {
	idMappers = make(map[string]*IdMapper)
	idMappersMutex = &sync.RWMutex{}
}

func mapperTableName(mapperName string) string {
	if len(tablePrefix) == 0 {
		return fmt.Sprintf("idmap.%s", mapperName)
	} else {
		return fmt.Sprintf("%s.idmap.%s", tablePrefix, mapperName)
	}
}
