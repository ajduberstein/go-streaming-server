package main

import (
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type EpochRecord struct {
	epoch   int64
	payload string
}

func main() {
	/*
		Example file input format:
		epoch,payload
		1440358702,"{ ""device_id"": 1, ""capacity"": 10 }"
		...

		Example use:

	*/
	fmt.Println("Launching server...")
	ln, _ := net.Listen("tcp", ":8081")
	fileName := os.Args[1]
	timerStart, _ := strconv.ParseInt(os.Args[2], 10, 64)
	conn, _ := ln.Accept()
	//Initial state
	timer := timerStart
	var playbackData map[int64]string
	f := strings.NewReader(fileName)
	r := csv.NewReader(f)

	//Create list
	for {
		if parts, err := r.Read(); err == nil {
			epoch, _ := strconv.ParseInt(parts[0], 10, 64)
			cs := EpochRecord{epoch, parts[1]}
			playbackData[cs.epoch] = cs.payload
		} else {
			break
		}
	}

	for {
		timer += 1
		newMessage := playbackData[timer] + "\n"
		time.Sleep(1000 * time.Millisecond)
		conn.Write([]byte(newMessage))
	}
}
