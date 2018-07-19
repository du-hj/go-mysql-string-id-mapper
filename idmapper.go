package idmapper

import (
	"database/sql"
	"errors"
	"log"
	"sync"
)

var (
	ErrorNotFound = errors.New("Not found")
)

const (
	INVALID = uint32(0xffffffff)
)

type IdMapper struct {
	tableName  string
	s2id       map[string]uint32
	id2s       []string
	mutex      *sync.RWMutex
	maxKeySize int
}

func Init(dbConnString, prefix string) (err error) {
	tablePrefix = prefix

	db, err = sql.Open("mysql", dbConnString)
	if err != nil {
		log.Fatalf("[idmapper] Open sql error: %+v", err)
		return
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("[idmapper] Ping db error: %+v", err)
		return
	}

	return
}

func GetIdMapper(mapperName string, createTableIfNotExist bool) *IdMapper {
	if len(mapperName) == 0 {
		log.Fatalf("[idmapper] GetIdMapper mapperName is empty")
		return nil
	}

	// Get idmapper from memroy cache
	idMappersMutex.RLock()
	if idMapper, has := idMappers[mapperName]; has {
		idMappersMutex.RUnlock()
		return idMapper
	}
	idMappersMutex.RUnlock()

	// idmapper has not been cached in memroy
	idMapper := newIdMapper(mapperName)

	// Try to load data from table, if the table does not exist, loadFromTable() returns ErrorNotFound
	if e := idMapper.loadFromTable(); e == nil {
		idMappersMutex.Lock()
		idMappers[mapperName] = idMapper
		idMappersMutex.Unlock()

		return idMapper
	} else {
		if e == ErrorNotFound {
			if createTableIfNotExist {
				if e2 := idMapper.createTable(); e2 == nil {
					log.Printf("[idmapper] mapper table '%s' has been created", idMapper.tableName)

					idMappersMutex.Lock()
					idMappers[mapperName] = idMapper
					idMappersMutex.Unlock()

					return idMapper
				} else {
					log.Fatalf("[idmapper] create mapper table '%s' error: +%v", idMapper.tableName, e2)
				}
			}
		} else {
			log.Fatal("[idmapper] loadFromTable error: %+v", e)
		}
	}

	return nil
}

func (mapper *IdMapper) HasItem(item string) (uint32, bool) {
	if len(item) > mapper.maxKeySize {
		item = item[:mapper.maxKeySize]
	}

	mapper.mutex.RLock()
	defer mapper.mutex.RUnlock()

	if idx, has := mapper.s2id[item]; has {
		return idx, true
	} else {
		return 0, false
	}
}

func (mapper *IdMapper) IdFromItem(item string, addIfNotExist bool) uint32 {
	if len(item) > mapper.maxKeySize {
		item = item[:mapper.maxKeySize]
	}

	mapper.mutex.RLock()
	if idx, has := mapper.s2id[item]; has {
		mapper.mutex.RUnlock()
		return idx
	}
	mapper.mutex.RUnlock()

	if !addIfNotExist {
		return INVALID
	}

	mapper.mutex.Lock()
	defer mapper.mutex.Unlock()

	if idx, has := mapper.s2id[item]; has {
		return idx
	}

	if idx, err := mapper.insertItem(item); err != nil {
		log.Fatalf("[idmapper] Insert item '%s' to table '%s' error: %+v", item, mapper.tableName, err)
		return INVALID

	} else {
		if idx != INVALID {
			mapper.s2id[item] = idx
			for int(idx) >= len(mapper.id2s) {
				/* Extend the map table by 100 elements */
				mapper.id2s = append(mapper.id2s, make([]string, 100)...)
			}
			mapper.id2s[idx] = item
		}
		return idx
	}
}

func (mapper *IdMapper) ItemFromId(idx uint32) (string, bool) {
	mapper.mutex.RLock()
	defer mapper.mutex.RUnlock()

	if int(idx) > len(mapper.s2id) || idx == 0 {
		return "", false
	} else {
		return mapper.id2s[idx], true
	}
}

func (mapper *IdMapper) Items() []string {
	mapper.mutex.RLock()
	defer mapper.mutex.RUnlock()

	items := make([]string, 0, len(mapper.s2id))

	for s, _ := range mapper.s2id {
		if len(s) > 0 {
			items = append(items, s)
		}
	}

	return items
}
