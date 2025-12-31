package main

import (
	"github.com/pixperk/chug/cmd"
	"github.com/pixperk/chug/internal/logx"
)

func main() {
	logx.InitLogger()
	cmd.Execute()
}
