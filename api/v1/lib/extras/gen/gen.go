// +build ignore

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
)

type Config struct {
	Package         string
	Imports         []string
	EventType       string
	EventPrototype  string
	ReturnType      string
	ReturnPrototype string
	Args            string // arguments that we were invoked with
}

func (c *Config) String() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%#v", ([]string)(c.Imports))
}

func (c *Config) Set(s string) error {
	c.Imports = append(c.Imports, s)
	return nil
}

func (c *Config) ReturnVar(names ...string) string {
	if c.ReturnType == "" || len(names) == 0 {
		return ""
	}
	return "var " + strings.Join(names, ",") + " " + c.ReturnType
}

func (c *Config) ReturnArg(name string) string {
	if c.ReturnType == "" {
		return ""
	}
	if name == "" {
		return c.ReturnType
	}
	if strings.HasSuffix(name, ",") {
		return strings.TrimSpace(name[:len(name)-1]+" "+c.ReturnType) + ", "
	}
	return name + " " + c.ReturnType
}

func (c *Config) ReturnRef(name string) string {
	if c.ReturnType == "" || name == "" {
		return ""
	}
	if strings.HasSuffix(name, ",") {
		if len(name) < 2 {
			panic("expected ref name before comma")
		}
		return name[:len(name)-1] + ", "
	}
	return name
}

func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Package, "package", c.Package, "destination package")
	fs.StringVar(&c.EventType, "event_type", c.EventType, "golang type of the event to be processed")
	fs.StringVar(&c.ReturnType, "return_type", c.ReturnType, "golang type of a return arg")
	fs.StringVar(&c.ReturnPrototype, "return_prototype", c.ReturnPrototype, "golang expression of a return obj prototype")
	fs.Var(c, "import", "packages to import")
}

func NewConfig() *Config {
	var (
		c = Config{
			Package:   os.Getenv("GOPACKAGE"),
			EventType: "Event",
		}
	)
	return &c
}

func Run(src, test *template.Template, args ...string) {
	if len(args) < 1 {
		panic(errors.New("expected at least one arg"))
	}
	var (
		c             = NewConfig()
		defaultOutput = "foo.go"
		output        string
	)
	if c.Package != "" {
		defaultOutput = c.Package + "_generated.go"
	}

	fs := flag.NewFlagSet(args[0], flag.PanicOnError)
	fs.StringVar(&output, "output", output, "path of the to-be-generated file")
	c.AddFlags(fs)

	if err := fs.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			fs.PrintDefaults()
		}
		panic(err)
	}

	c.Args = strings.Join(args[1:], " ")

	if c.Package == "" {
		c.Package = "foo"
	}
	if c.EventType == "" {
		c.EventType = "Event"
		c.EventPrototype = "Event{}"
	} else if strings.HasPrefix(c.EventType, "*") {
		// TODO(jdef) don't assume that event type is a struct or *struct
		c.EventPrototype = "&" + c.EventType[1:] + "{}"
	} else {
		c.EventPrototype = c.EventType[1:] + "{}"
	}
	if c.ReturnType != "" && c.ReturnPrototype == "" {
		panic(errors.New("return_prototype is required when return_type is set"))
	}

	if output == "" {
		output = defaultOutput
	}

	genmap := make(map[string]*template.Template)
	if src != nil {
		genmap[output] = src
	}
	if test != nil {
		testOutput := output + "_test"
		if strings.HasSuffix(output, ".go") {
			testOutput = output[:len(output)-3] + "_test.go"
		}
		genmap[testOutput] = test
	}
	if len(genmap) == 0 {
		panic(errors.New("neither src or test templates were provided"))
	}

	Generate(genmap, c, func(err error) { panic(err) })
}

func Generate(items map[string]*template.Template, data interface{}, eh func(error)) {
	for filename, t := range items {
		func() {
			f, err := os.Create(filename)
			if err != nil {
				eh(err)
				return
			}
			defer f.Close()
			log.Println("generating file", filename)
			err = t.Execute(f, data)
			if err != nil {
				eh(err)
			}
		}()
	}
}
