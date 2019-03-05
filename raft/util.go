package raft

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"time"
)

// Debugging
const Debug = -2
var Flag  = 0

func init(){
	// Output to stdout instead of the default stderr
	// Output file name and line number
	log.SetOutput(os.Stdout)
	//log.SetReportCaller(true)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: time.RFC3339Nano,
	})
}
// 调试信息
func DPrintf5(format string, a ...interface{}) (n int, err error) {

	if Debug > -1  && Flag ==1 {
		log.Debugf(format, a...)
	}
	return
}
// 核心状态转变
func DPrintf4(format string, a ...interface{}) (n int, err error) {

	if Debug > 0  && Flag ==1 {
		log.Printf(format, a...)
	}
	return
}
// 动作信息
func DPrintf3(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 && Flag ==1{
		log.Printf(format, a...)
	}
	return
}
func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug > 2 && Flag ==1{
		log.Printf(format, a...)
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 3 && Flag ==1 {
		log.Printf(format, a...)
	}
	return
}
func ADPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func randInt(low, upper int) int {
	randNum := rand.Intn(upper-low) + low
	return randNum
}

