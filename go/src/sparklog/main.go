package main

import (
	"encoding/json"
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"html/template"
	"net/http"
	"sparklog/fileParser"
	"strconv"
)

var stringData *string
var stringHtml *string
var port int = 8080

var currentFile string

func startServer(html string) {
	r := mux.NewRouter()

	r.HandleFunc("/logList", getLogList)
	r.HandleFunc("/currentLog", getCurrentLog)
	r.HandleFunc("/analyze", analyze)
	r.HandleFunc("/EVENTLOG_1", getLog)
	r.HandleFunc("/", index)

	r.PathPrefix("/").Handler(http.FileServer(http.Dir(html)))
	http.Handle("/", r)
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

func analyze(w http.ResponseWriter, r *http.Request) {
	currentFile = r.FormValue("path")
	log.Println("Path to analyze ", currentFile)
	// t, err := template.ParseFiles("web/analyze.html")
	// if err != nil {
	// 	log.Println(err)
	// }
	// t.Execute(w, p)
}

func index(w http.ResponseWriter, _ *http.Request) {
	log.Info("Hello")
	t, err := template.ParseFiles("web/index.html")
	if err != nil {
		log.Println(err)
	}
	t.Execute(w, fileParser.FileDate)
}

func getLog(w http.ResponseWriter, r *http.Request) {
	log.Info("GetLog")
	http.ServeFile(w, r, currentFile)
	//fileParser.Dump()
}

func getLogList(w http.ResponseWriter, r *http.Request) {
	t := fileParser.GetAsArray()
	js, err := json.Marshal(t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func getCurrentLog(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(currentFile))
}

func main() {
	log.SetLevel(log.InfoLevel)
	log.Info("Spark log analyzer running on port ", port)

	stringData = flag.String("data", ".", "data directory")
	stringHtml = flag.String("html", "web", "html directory")

	flag.Parse()
	log.Info("Option data : ", *stringData)
	log.Info("Option html : ", *stringHtml)
	fileParser.FindFiles(*stringData)
	startServer(*stringHtml)
}
