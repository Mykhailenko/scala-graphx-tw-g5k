package fileParser

import (
	"bufio"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//used for jsong parsing
type StartEvent struct {
	Timestamp int64
}

type LogFile struct {
	Timestamp time.Time
	Path      string
}

var FileDate map[time.Time]string

func keepFile(f *os.File) {
	scanner := bufio.NewScanner(f)
	//for scanner.Scan() {
	scanner.Scan()
	//log.Info(scanner.Text())
	line := scanner.Text()
	if strings.Contains(line, "{\"Event\"") {
		log.Debug("Found log file " + f.Name())
		//now extract the date, look for SparkListenerApplicationStart event
		for scanner.Scan() {
			line = scanner.Text()
			if strings.Contains(line, "SparkListenerApplicationStart") {
				log.Debug("Found SparkListenerApplicationStart event")
				var ev StartEvent
				err := json.Unmarshal([]byte(line), &ev)
				if err == nil {
					t := time.Unix(ev.Timestamp/1000, 0)
					//log.Info(ev.Timestamp)
					log.Debug(t)
					FileDate[t] = f.Name()
				} else {
					log.Error(err)
					log.Error(ev)
				}
				return
			}
		}

	}
}

func GetAsArray() []LogFile {
	tmp := make([]LogFile, len(FileDate))
	idx := 0
	for key, value := range FileDate {
		tmp[idx] = LogFile{Timestamp: key, Path: value}
		idx++
	}
	return tmp
}

func visit(path string, f os.FileInfo, err error) error {
	log.Debug("Visited: " + path)

	if f.IsDir() {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	keepFile(file)

	return nil
}

func Dump() {
	for k, v := range FileDate {
		log.Info(k, " -> ", v)
	}

}

func FindFiles(path string) {
	FileDate = make(map[time.Time]string)
	filepath.Walk(path, visit)
	//Dump()
}
