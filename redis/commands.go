package redis

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
