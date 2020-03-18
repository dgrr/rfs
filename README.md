# rfs


```go
package main

import (
	"fmt"
	"log"

	"github.com/dgrr/rfs"
	_ "github.com/dgrr/rfs/s3"
)

func main() {
	fs, err := rfs.Dial("s3", "my-bucket", nil)
	if err != nil {
		log.Fatalln("dial error:", err)
	}

	file, err := fs.Create("/dgrr/hello")
	if err != nil {
		log.Fatalln("open error:", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "test")
}
```
