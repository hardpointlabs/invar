package redis

import (
	"strconv"

	"github.com/tidwall/redcon"
)

type SetCommand string

const (
	Nx      SetCommand = "nx"
	Xx      SetCommand = "xx"
	Ifeq    SetCommand = "ifeq"
	Ifne    SetCommand = "ifne"
	Ifdeq   SetCommand = "ifdeq"
	Ifdne   SetCommand = "ifdne"
	Ex      SetCommand = "ex"
	Px      SetCommand = "px"
	Exat    SetCommand = "exat"
	Pxat    SetCommand = "pxat"
	KeepTtl SetCommand = "keepttl"
)

func checkExactArgs(conn redcon.Conn, cmd redcon.Command, n int) bool {
	if len(cmd.Args) != n {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return false
	}
	return true
}

func checkMinArgs(conn redcon.Conn, cmd redcon.Command, n int) bool {
	if len(cmd.Args) < n {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return false
	}
	return true
}

func parseIntArg(conn redcon.Conn, arg []byte) (int, bool) {
	i, err := strconv.Atoi(string(arg))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return 0, false
	}
	return i, true
}

func parseInt64Arg(conn redcon.Conn, arg []byte) (int64, bool) {
	i, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return 0, false
	}
	return i, true
}

func parseFloatArg(conn redcon.Conn, arg []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(arg), 64)
	if err != nil {
		conn.WriteError("ERR value is not a float")
		return 0, false
	}
	return f, true
}
