package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	SetLoggerConsole(false)
}

var ColourDisabled bool

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
)

// Helper for escape analysis; avoids go thinking the variadic argument escapes.
// Default "verb" behaviour.
func V[T any](copyThatEscapes T) string {
	return fmt.Sprintf("%v", copyThatEscapes)
}

// Helper for escape analysis; avoids go thinking the variadic argument escapes.
// Uses the given format string.
func F[T any](f string, copyThatEscapes T) string {
	return fmt.Sprintf(f, copyThatEscapes)
}

func colorize(s interface{}, c int) string {
	if ColourDisabled {
		return fmt.Sprintf("%s", s)
	}
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}

func SetLevel(level int) {
	switch level {
	case 0:
		log.Logger = log.With().Logger().Level(zerolog.InfoLevel)
	case 1:
		log.Logger = log.With().Logger().Level(zerolog.DebugLevel)
	default:
		log.Logger = log.With().Logger().Level(zerolog.TraceLevel)
	}
}

func SetLoggerConsole(noColour bool) {
	ColourDisabled = noColour
	zerolog.CallerMarshalFunc = callerMarshal

	cw := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.TimeOnly, NoColor: noColour}
	cw.FormatCaller = consoleFormatCaller
	cw.FormatLevel = consoleFormatLevel
	cw.PartsOrder = []string{
		zerolog.TimestampFieldName,
		zerolog.CallerFieldName,
		zerolog.LevelFieldName,
		zerolog.MessageFieldName,
	}
	log.Logger = log.With().Caller().Logger().Output(cw)
}

func callerMarshal(pc uintptr, file string, line int) string {
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = fmt.Sprintf("%15s.%-4s", short, strconv.Itoa(line))
	if len(file) > 20 {
		file = ".." + file[len(file)-18:]
	}
	return colorize(file, colorBlack)
}

func consoleFormatCaller(i any) string {
	var c string
	if cc, ok := i.(string); ok {
		c = cc
	}
	if len(c) > 0 {
		if cwd, err := os.Getwd(); err == nil {
			if rel, err := filepath.Rel(cwd, c); err == nil {
				c = rel
			}
		}
		c = colorize(c, colorBold)
	}
	return c
}

func consoleFormatLevel(i any) string {
	var l string
	if ll, ok := i.(string); ok {
		switch ll {
		case zerolog.LevelTraceValue:
			l = colorize("| TRACE |", colorMagenta)
		case zerolog.LevelDebugValue:
			l = colorize("| DEBUG |", colorYellow)
		case zerolog.LevelInfoValue:
			l = colorize("| INFO  |", colorGreen)
		case zerolog.LevelWarnValue:
			l = colorize("| WARN  |", colorRed)
		case zerolog.LevelErrorValue:
			l = colorize(colorize("| ERROR |", colorRed), colorBold)
		case zerolog.LevelFatalValue:
			l = colorize(colorize("| FATAL |", colorRed), colorBold)
		case zerolog.LevelPanicValue:
			l = colorize(colorize("| PANIC |", colorRed), colorBold)
		default:
			l = colorize(ll, colorBold)
		}
	} else {
		if i == nil {
			l = colorize("| ??? |", colorBold)
		} else {
			l = strings.ToUpper(fmt.Sprintf("| %5s |", i))
		}
	}
	return l
}

func MemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Debug().Msg("(MiB): Alloc: " + V(m.Alloc/1024/1024) + " Sys: " + V(m.Sys/1024/1024) +
		" TotalAlloc: " + V(m.TotalAlloc/1024/1024) +
		" HeapInuse: " + V(m.HeapInuse/1024/1024) +
		//" StackSys: " + V(m.StackSys/1024/1024) +
		". (#): NumGC: " + V(m.NumGC))
}
