package main

import ("fmt"
	"strings"
	"bufio"
	"log"
	"os/exec"
	"bytes"
	"time"
	"os"
	"github.com/marpaia/graphite-golang"
	"strconv"
)
const (
	utc_layout  = "2006-01-02 15:04:05 UTC"
)

func conv_float (s string) float64 {
	i ,_ := strconv.ParseFloat(s, 64)
	return i
}
func check_index_exist(line string, item_index int ) string {
	result_item := "NOTHING"
	slice_line := strings.Split(line, ";")
	if strings.ContainsAny(slice_line[0], "#") != true {
		if len(slice_line) > 4 && item_index > 3 && strings.ContainsAny(slice_line[item_index], ","){
			return strings.Replace(slice_line[item_index], ",", ".", -1)
		}
		if len(slice_line) > 4{
			return slice_line[item_index]
		}
	}
	return result_item
}
func get_cpu (sadf_args []string) []graphite.Metric {
	cpu_graphite_metrics := make([]graphite.Metric, 0)
	run_sadf := exec.Command("sadf", sadf_args...)
	run_sadf_output, run_err := run_sadf.Output()
	if run_err != nil {
		log.Panic(run_err)
	}
	out_scaner := bufio.NewScanner(bytes.NewReader(run_sadf_output))
	for out_scaner.Scan() {
		parse_line := out_scaner.Text()
		if check_index_exist(parse_line, 0) != "NOTHING" {
		timestamp, get_time_err := time.Parse(utc_layout, check_index_exist(parse_line, 2))
		if get_time_err != nil {
			log.Fatal(get_time_err)
		}
		hostname := check_index_exist(parse_line, 0)
		path_cpu := hostname + "." + "CPU" + "."
		iowait := check_index_exist(parse_line,7)
		user := check_index_exist(parse_line, 4)
		nice := check_index_exist(parse_line, 5)
		idle := check_index_exist(parse_line, 9)
		system := check_index_exist(parse_line, 6)
		steal := check_index_exist(parse_line, 8)
		cpu_graphite_metrics = append(cpu_graphite_metrics, graphite.NewMetric(path_cpu+ "iowait", iowait, timestamp.Unix()),
					graphite.NewMetric(path_cpu+ "user", user, timestamp.Unix()),
					graphite.NewMetric(path_cpu+ "nice", nice, timestamp.Unix()),
					graphite.NewMetric(path_cpu+ "idle", idle, timestamp.Unix()),
					graphite.NewMetric(path_cpu+ "system", system, timestamp.Unix()),
					graphite.NewMetric(path_cpu+ "steal", steal, timestamp.Unix()))
		}
	}
	return cpu_graphite_metrics
}
func get_disk (sadf_args []string) []graphite.Metric {
	graphite_disk_metrics := make([]graphite.Metric, 0, 0)
	run_sadf := exec.Command("sadf", sadf_args...)
	run_sadf_output, run_err := run_sadf.Output()
	if run_err != nil {
		log.Panic(run_err)
	}
	out_scaner := bufio.NewScanner(bytes.NewReader(run_sadf_output))
	for out_scaner.Scan() {
		parse_line := out_scaner.Text()
		if check_index_exist(parse_line, 0) != "NOTHING" {
			timestamp, get_time_err := time.Parse(utc_layout, check_index_exist(parse_line, 2))
			if get_time_err != nil {
				log.Fatal(get_time_err)
			}
			hostname := check_index_exist(parse_line, 0)
			path_disk := hostname + "." + "DISK" + "."
			dev_name := check_index_exist(parse_line,3 )
			r_blocks := check_index_exist(parse_line, 5)
			w_blocks := check_index_exist(parse_line, 6)
			pts := check_index_exist(parse_line, 4)
			await := check_index_exist(parse_line, 9)
			util := check_index_exist(parse_line, 11)
			//Calc in MB/s
			rd_mb := conv_float(r_blocks) * 512 / 1024 / 1024
			wr_mb := conv_float(w_blocks) * 512 / 1024 / 1024

			graphite_disk_metrics = append(graphite_disk_metrics,
				graphite.NewMetric(path_disk + "r_blocks." + dev_name, r_blocks, timestamp.Unix()),
				graphite.NewMetric(path_disk + "w_blocks." + dev_name, w_blocks, timestamp.Unix()),
				graphite.NewMetric(path_disk + "pts." + dev_name, pts, timestamp.Unix()),
				graphite.NewMetric(path_disk + "await." + dev_name, await, timestamp.Unix()),
				graphite.NewMetric(path_disk + "util." + dev_name, util, timestamp.Unix()),
				graphite.NewMetric(path_disk + "rd_mb." + dev_name, strconv.FormatFloat(rd_mb, 'g', -1, 64), timestamp.Unix()),
				graphite.NewMetric(path_disk + "wr_mb." + dev_name, strconv.FormatFloat(wr_mb, '8', -1, 64), timestamp.Unix()))

				}
	}
	return graphite_disk_metrics
}
func main() {
	if len(os.Args) > 1 {
		log_path := os.Args[1]
		disk_stat := []string{"-d", log_path, "--", "-d", "-p"}
		cpu_stat := []string{"-d", log_path, "--", "-u"}
		if len(os.Args) > 2 {
			graphite_settings := strings.Split(os.Args[2], ":")
			graphite_server := graphite_settings[0]
			graphite_port, _   := strconv.Atoi(graphite_settings[1])
			graphite_prefix, graphite_err := graphite.NewGraphite(graphite_server, graphite_port)
			if graphite_err != nil {
				log.Panic(graphite_err)
			}
			connect_err := graphite_prefix.Connect()
			if connect_err != nil {
				log.Fatal(connect_err)
			}
			if len(os.Args) > 3 {

				switch give_arg := os.Args[3]; give_arg {
				case "-CPU":
					metr := get_cpu(cpu_stat)
					for i, v := range metr {
						fmt.Println(i)
						fmt.Println(v)
					}
					graphite_prefix.SendMetrics(metr)
				case "-DISK":
					d := get_disk(disk_stat)
					for i, v := range d {
						fmt.Println(i)
						fmt.Println(v)
					}
					graphite_prefix.SendMetrics(d)
				default:
					fmt.Println("-CPU or -DISK")

				}
			} else {
				fmt.Printf("%s", "YOU MUST GIVE METRICS TYPE ARGUMENTS!!")
			}
		} else {
			fmt.Printf("%s", "YOU MUST GIVE GRAPHITE CONNECTION SETTINGS!")
		}
		} else {
		fmt.Printf("%s", "YOU MSUT GIVE LOG PATH!")
	}

}

