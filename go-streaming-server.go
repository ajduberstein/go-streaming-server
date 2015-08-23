package main

import (
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"strconv"
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
		go-streaming-server example.csv 1440358701
	*/
	fmt.Println("Launching server...")
	ln, _ := net.Listen("tcp", ":8081")
	fileName := os.Args[1]
	timerStart, _ := strconv.ParseInt(os.Args[2], 10, 64)
	timerEnd, _ := strconv.ParseInt(os.Args[3], 10, 64)
	//Initial state
	playbackData := make(map[int64]string)
	file, _ := os.Open(fileName)
	defer file.Close()
	r := csv.NewReader(file)
	rows, _ := r.ReadAll()
	conn, _ := ln.Accept()
	//Create list
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		epoch, _ := strconv.ParseInt(row[0], 10, 64)
		cs := EpochRecord{epoch, row[1]}
		playbackData[cs.epoch] = cs.payload
	}
	for t := timerStart; ; t++ {
		newMessage := playbackData[t] + "\n"
		time.Sleep(1000 * time.Millisecond)
		conn.Write([]byte(newMessage))
		if t > timerEnd {
			t = timerStart
		}
	}
}
