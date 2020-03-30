package dlog

import (
	"log"
	"os"
)

const DLOG = false

func Printf(format string, v ...interface{}) {
	if !DLOG {
		return
	}
	log.Printf(format, v...)
}

func Println(v ...interface{}) {
	if !DLOG {
		return
	}
	log.Println(v...)
}

func NewFileLogger(file *os.File) *log.Logger {
	return log.New(file, "", log.LstdFlags)
}
