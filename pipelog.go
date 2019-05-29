package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PaesslerAG/jsonpath"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"
)

var timeOutput = "2006-01-02"

var (
	piped    bool
	duration = kingpin.Flag("duration", "Path to request duration").Short('d').Default("duration").String()
	uri      = kingpin.Flag("uri", "Path to request uri").Short('u').Default("uri").String()
	ts       = kingpin.Flag("time", "Path to request timestamp").Short('t').Default("time").String()
	ns       = kingpin.Flag("namespace", "JSON path to apply to all other fields").Short('n').Default("$").String()
	file     = kingpin.Flag("file", "Log file to be parsed").ExistingFile()
	grouped  = kingpin.Flag("grouped", "Whether request should be grouped by uri").Short('g').Bool()
	fail     = kingpin.Flag("fail", "Fail if an error occurs while parsing a single line").Short('F').Bool()
)

func main() {
	var (
		tmpIf       interface{}
		tmpData     interface{}
		tmpString   string
		reqDuration float64
		reqTime     time.Time
		reqURI      string
		ok          bool
		err         error
		input       []byte
	)
	kingpin.UsageTemplate(kingpin.CompactUsageTemplate).Version("1.0").Author("Michele Finotto")
	kingpin.CommandLine.Help = "Pipelog"
	kingpin.Parse()

	if len(*ns) > 0 {
		duration = addNamespace(ns, duration)
		ts = addNamespace(ns, ts)
		uri = addNamespace(ns, uri)
	}

	info, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if info.Mode()&os.ModeCharDevice != 0 && len(*file) == 0 {
		fmt.Println("You should either pipe something into pipelog or specify and existing file to parse")
		// return
	}

	durations := map[string][]float64{}
	uris := map[string][]float64{}

	reader := bufio.NewReader(os.Stdin)

	for {
		input, err = reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if input[0] != '{' {
			continue
		}
		tmpIf = interface{}(nil)

		json.Unmarshal(input, &tmpIf)

		tmpData, err = jsonpath.Get(*duration, tmpIf)
		if err != nil {
			if *fail {
				fmt.Println(err)
				os.Exit(1)
			}
			continue
		}
		reqDuration, ok = tmpData.(float64)

		if !ok {
			tmpString, ok = tmpData.(string)
			reqDuration, err = strconv.ParseFloat(tmpString, 64)
			if err != nil {
				if *fail {
					fmt.Println(err)
					os.Exit(1)
				}
				continue
			}
		}

		tmpData, err = jsonpath.Get(*uri, tmpIf)
		if err != nil {
			if *fail {
				fmt.Println(err)
				os.Exit(1)
			}
			continue
		}

		reqURI, ok = tmpData.(string)
		if !ok {
			if *fail {
				fmt.Println("Couldn't parse uri")
				os.Exit(1)
			}
			continue
		}

		tmpData, err = jsonpath.Get(*ts, tmpIf)
		if err != nil {
			if *fail {
				fmt.Println(err)
				os.Exit(1)
			}
			continue
		}
		tmpString, ok = tmpData.(string)
		if !ok {
			if *fail {
				fmt.Println("Couldn't parse timestamp")
				os.Exit(1)
			}
			continue
		}
		reqTime, err = time.Parse(time.RFC3339, tmpString)
		if err != nil {
			if *fail {
				fmt.Println(err)
				os.Exit(1)
			}
			continue
		}

		addDuration(durations, reqTime.Format(timeOutput), reqDuration)
		addDuration(uris, cleanURI(reqURI), reqDuration)
	}
	printMap("Day", durations, false, 0)
	printMap("URI", uris, true, 20)
}

func addNamespace(ns, path *string) *string {
	tmp := fmt.Sprintf("%s.%s", *ns, *path)
	return &tmp
}

func addDuration(theMap map[string][]float64, key string, duration float64) {
	reqs, ok := theMap[key]
	if !ok {
		reqs = []float64{}
	}
	reqs = append(reqs, duration)
	theMap[key] = reqs
}

func cleanURI(uri string) string {
	return strings.Split(uri, "?")[0]
}

func printMap(title string, theMap map[string][]float64, sorted bool, limit int) {
	lines := []*statLine{}
	for k, data := range theMap {
		lines = append(lines, newStatLine(k, data))
	}
	if sorted {
		sort.Slice(lines, func(i, j int) bool { return lines[i].reqs > lines[j].reqs })
	}
	if limit == 0 || limit > len(lines) {
		limit = len(lines)
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(StatLineHeaders(title))

	for i := 0; i < limit; i++ {
		table.Append(lines[i].Array())
	}
	table.Render()
}

type statLine struct {
	key   string
	data  []float64
	ntile float64
	avg   float64
	min   float64
	max   float64
	reqs  int
}

func newStatLine(key string, data []float64) *statLine {
	sort.Float64s(data)
	tile := int(float64(95*len(data)) / 100.0)
	var total float64 = 0
	for _, value := range data {
		total += value
	}
	return &statLine{
		key:   key,
		data:  data,
		ntile: data[tile],
		avg:   total / float64(len(data)),
		min:   data[0],
		max:   data[len(data)-1],
		reqs:  len(data),
	}
}

func (sl statLine) String() string {
	return fmt.Sprintf("%s: %d reqs; avg. %fms; min. %fms; max. %fms; 95th %fms", sl.key, sl.reqs, sl.avg, sl.min, sl.max, sl.ntile)
}

func StatLineHeaders(title string) []string {
	return []string{title, "Reqs", "Avg", "Min", "Max", "95th"}
}

func (sl statLine) Array() []string {
	return []string{
		sl.key,
		fmt.Sprintf("%d", sl.reqs),
		fmt.Sprintf("%.3fms", sl.avg),
		fmt.Sprintf("%.3fms", sl.min),
		fmt.Sprintf("%.3fms", sl.max),
		fmt.Sprintf("%.3fms", sl.ntile),
	}
}
