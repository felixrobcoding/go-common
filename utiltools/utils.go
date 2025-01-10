package utiltools

import (
	"log"
	"runtime/debug"
)

func ExceptionCatch() {
	if err := recover(); err != nil {
		log.Println("Exception err:", err, " \n stack:", string(debug.Stack()))
	}
}
