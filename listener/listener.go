package main
import (
	"net/http"
	"github.com/EyciaZhou/configparser"
)

/*
it listen port(maybe 80, can be configure in conf file), to hold a pictures server
though apache also progresses well, but i may write something like scale,
rotate or blur the pictures
someday, so i decides write server myself
*/

type Config struct {
	RootPath string `default:"/data/pic"`
	ListenPort string `default:":8080"`
}

var (
	config Config
)

func main() {
	configparser.AutoLoadConfig("listener", &config)
	http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(config.RootPath))))
	http.ListenAndServe(config.ListenPort, nil)
}

