package main

import ("fmt"
	"strings"
	"bufio"
	"log"
	"os/exec"
	"bytes"
	"time"
	"os"
)
const (
	utc_layout  = "2006-01-02 15:04:05 UTC"
)
var (
	log_path string = "/var/log/sysstat/sa18"
	disk_stat = []string{"-d", log_path, "--", "-d", "-p"}
	cpu_stat = []string{"-d", log_path, "--", "-u"}

)
type CpuMetric struct {
	timestamp 	int64
	iowait  	string
	user		string
	nice		string
	idle		string
	system		string
	steal		string
}
type DiskMetric struct {
	timestamp		int64
	r_blocks		string
	w_blocks		string
	pts			string
	await			string
	util			string
	//	w_mb			string
	//	r_mb			string
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
func get_cpu (sadf_args []string) []CpuMetric {
	cpu_metric_list := make([]CpuMetric, 0, 0)
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
			cpu_metric_list = append(cpu_metric_list, CpuMetric{timestamp.Unix(),
				check_index_exist(parse_line,7),
				check_index_exist(parse_line, 4),
				check_index_exist(parse_line,5),
				check_index_exist(parse_line, 9),
				check_index_exist(parse_line, 6),
				check_index_exist(parse_line, 8)})
		}
	}
	return cpu_metric_list
}

func get_disk (sadf_args []string) []DiskMetric {
	disk_metric_list := make([]DiskMetric, 0, 0)
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
			disk_metric_list = append(disk_metric_list, DiskMetric{timestamp.Unix(),
			check_index_exist(parse_line, 5),
			check_index_exist(parse_line, 6),
			check_index_exist(parse_line, 4),
			check_index_exist(parse_line, 9),
			check_index_exist(parse_line,11)})
		}
	}
	return disk_metric_list
}

func main() {
		if len(os.Args) > 3 {
			switch give_arg := os.Args[3]; give_arg {
			case "-cpu":
				metr := get_cpu(cpu_stat)
				for i, v := range  metr{
					fmt.Println(i)
					fmt.Println(v.timestamp)
				}
			case "-disk":
				for i, v := range (get_disk(disk_stat)){
					fmt.Println(i)
					fmt.Println(v)
				}
			default: fmt.Println("WRONG OPTIONS!")




			}
		} else {
			fmt.Printf("%s", "YOU MUST GIVE ARGUMENTS!!")
		}


		
}



