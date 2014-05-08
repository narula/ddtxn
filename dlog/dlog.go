package dlog

import (
	"flag"
	"log"
)

var Debug = flag.Bool("d", false, "turn on debug")

func Printf(s string, v ...interface{}) {
	if *Debug {
		log.Printf(s, v...)
	}
}

func Println(v ...interface{}) {
	if *Debug {
		log.Println(v)
	}
}

func Fatalf(s string, v ...interface{}) {
	if *Debug {
		log.Fatalf(s, v...)
	}
}

func FFatal(s string, v ...interface{}) {
	log.Fatalf(s, v...)
}
