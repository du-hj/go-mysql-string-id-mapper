# golang mysql string <-> id mapper

This lib depends on [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)

## Example

    package main

    import (
        "fmt"
        "github.com/du-hj/go-mysql-string-id-mapper"
    )

    func main() {
        if err := idmapper.Init("root:123456@/mydb?charset=utf8&loc=Local", "mydb"); err != nil {
            fmt.Printf("error: %+v", err)
        }

        m := idmapper.GetIdMapper("name", true)

        count := 20

        c := make(chan bool, count)

        for i := 0; i < count; i++ {
            name := fmt.Sprintf("name_%d", i+1)
            go func() {
                id := m.IdFromItem(name, true)
                fmt.Printf("new id for string item %s: %d\n", name, id)
                c <- true
            }()
        }

        // wait all routines done
        for i := 0; i < count; i++ {
            <-c
        }

        fmt.Println()
        fmt.Printf("all string items: %+v\n", m.Items())
        fmt.Println()

        for i := 0; i < count; i++ {
            name := fmt.Sprintf("name_%d", i+1)
            id := m.IdFromItem(name, false)
            fmt.Printf("existing id for string item %s: %d\n", name, id)
        }
        fmt.Println()

        for i := 0; i < count; i++ {
            id := uint32(i + 1)
            item, has := m.ItemFromId(id)
            fmt.Printf("id: %d item: %s has: %v\n", id, item, has)
        }
        fmt.Println()
    }
