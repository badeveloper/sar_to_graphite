package main

import (
	"strings"
	"bufio"
	"log"
	"os/exec"
	"bytes"
	"time"
	"os"
	"github.com/marpaia/graphite-golang"
	"github.com/fatih/color"
	"strconv"
)
const (
	utc_layout  = "2006-01-02 15:04:05 UTC"

)
var (
	err_color  = color.New(color.FgRed, color.Bold)
)

type Stat struct {
	name string
	args []string
}

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
				graphite.NewMetric(path_disk + "wr_mb." + dev_name, strconv.FormatFloat(wr_mb, 'g', -1, 64), timestamp.Unix()))

				}
	}
	return graphite_disk_metrics
}
func get_mem (sadf_args []string) []graphite.Metric {
	mem_graphite_metrics := make([]graphite.Metric, 0)
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
			path_mem := hostname + "." + "RAM" + "."
			kb_mem_free := check_index_exist(parse_line,3)
			kb_mem_used := check_index_exist(parse_line, 4)
			pec_used := check_index_exist(parse_line, 5)
			kb_buff := check_index_exist(parse_line, 6)
			kb_cach := check_index_exist(parse_line, 7)
			kb_comm := check_index_exist(parse_line, 8)
			pec_comm := check_index_exist(parse_line, 9)

			mem_graphite_metrics = append(mem_graphite_metrics, graphite.NewMetric(path_mem+ "kb_mem_free", kb_mem_free, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "kb_mem_used", kb_mem_used, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "%used", pec_used, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "kb_buf", kb_buff, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "kb_cach", kb_cach, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "commit", kb_comm, timestamp.Unix()),
				graphite.NewMetric(path_mem+ "%commit", pec_comm, timestamp.Unix()))
		}
	}
	return mem_graphite_metrics
}
func get_swp (sadf_args []string) []graphite.Metric {
	swap_graphite_metrics := make([]graphite.Metric, 0)
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
			path_swap := hostname + "." + "SWAP" + "."
			kb_swp_free := check_index_exist(parse_line,3)
			kb_swp_used := check_index_exist(parse_line, 4)
			pec_swp_used := check_index_exist(parse_line, 5)
			kb_swp_cad := check_index_exist(parse_line, 6)
			pec_swp_cad := check_index_exist(parse_line, 6)
			swap_graphite_metrics = append(swap_graphite_metrics, graphite.NewMetric(path_swap+ "kb_swap_free", kb_swp_free, timestamp.Unix()),
				graphite.NewMetric(path_swap+ "kb_swap_used", kb_swp_used, timestamp.Unix()),
				graphite.NewMetric(path_swap+ "%swap_used", pec_swp_used, timestamp.Unix()),
				graphite.NewMetric(path_swap+ "kb_swp_cad", kb_swp_cad, timestamp.Unix()),
				graphite.NewMetric(path_swap+ "%swap_cad", pec_swp_cad, timestamp.Unix()))
		}
	}
	return swap_graphite_metrics
}
func get_net (sadf_args []string) []graphite.Metric {
	net_graphite_metrics := make([]graphite.Metric, 0)
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
			path_net := hostname + "." + "NET" + "."
			if_name := check_index_exist(parse_line, 3)

			rxpck_s := check_index_exist(parse_line,4)
			txpck_s := check_index_exist(parse_line, 5)
			rxkb_s := check_index_exist(parse_line, 6)
			txkb_s := check_index_exist(parse_line, 7)
			rxcmp_s := check_index_exist(parse_line, 8)
			txcmp_s := check_index_exist(parse_line, 9)
			rxmcst_s := check_index_exist(parse_line, 10)
			//pec_ifutil := check_index_exist(parse_line, 11)

			net_graphite_metrics = append(net_graphite_metrics,
				graphite.NewMetric(path_net+ if_name +".recive_pk_sec", rxpck_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".transmit_pk_sec", txpck_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".recive_kb_sec", rxkb_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".transmit_kb_sec", txkb_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".recive_compressed_pk_sec", rxcmp_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".transmit_compressed_pk_sec", txcmp_s, timestamp.Unix()),
				graphite.NewMetric(path_net+ if_name +".recive_mcst_pk_sec", rxmcst_s, timestamp.Unix()))
				//graphite.NewMetric(path_net+ if_name +"interface_util_%.", pec_ifutil, timestamp.Unix()
		}
	}
	return net_graphite_metrics
}
func main() {
	if len(os.Args) > 1 {
		log_path := os.Args[1]
		disk_stat:= Stat{"disk", []string{"-d", log_path, "--", "-d", "-p"}}
		cpu_stat := Stat{ "cpu", []string{"-d", log_path, "--", "-u"}}
		mem_stat := Stat{"mem", []string{"-d", log_path, "--", "-r"}}
		swp_stat := Stat{"swp",[]string{"-d", log_path, "--", "-S"}}
		net_stat := Stat{"net",[]string{"-d", log_path, "--", "-n", "DEV"}}
		all_stat := []Stat{disk_stat, cpu_stat, swp_stat, net_stat}
		if len(os.Args) > 2 {
			graphite_settings := strings.Split(os.Args[2], ":")
			if len(graphite_settings) > 1 {
				graphite_server := graphite_settings[0]
				graphite_port, _ := strconv.Atoi(graphite_settings[1])
				graphite_prefix, graphite_err := graphite.NewGraphite(graphite_server, graphite_port)
				con_err_col := color.New(color.FgRed, color.Bold)

				if graphite_err != nil {

					con_err_col.Printf("%s", "CONNECTION PREFIX ERROR!!!")
				}
				connect_err := graphite_prefix.Connect()
				if connect_err != nil {
					con_err_col.Printf("%s","CONNECTION ERROR!!")
				}

				if len(os.Args) > 3 {
					send_err_col := color.New(color.FgRed, color.Bold)
					send_col := color.New(color.FgMagenta, color.Bold)
					switch give_arg := os.Args[3]; give_arg {
					case "-CPU":
						graphite_prefix.SendMetrics(get_cpu(cpu_stat.args))
						send_col.Printf("%s", "Send CPU usage stat...")
					case "-DISK":
						graphite_prefix.SendMetrics(get_disk(disk_stat.args))
						send_col.Printf("%s", "Send DISK usage stat...")
					case "-RAM":
						graphite_prefix.SendMetrics(get_mem(mem_stat.args))
						send_col.Printf("%s", "Send RAM usage stat...")
					case "-SWP":
						graphite_prefix.SendMetrics(get_swp(swp_stat.args))
						send_col.Printf("%s", "Send SWAP usage stat...")
					case "-NET":
						graphite_prefix.SendMetrics(get_net(net_stat.args))
						send_col.Printf("%s", "Send NET_DEV usage stat..")
					case "-ALL":
						for _, stat := range all_stat {
							if stat.name == "disk" {
								graphite_prefix.SendMetrics(get_disk(disk_stat.args))
							}
							if stat.name == "cpu" {
								graphite_prefix.SendMetrics(get_cpu(stat.args))
							}
							if stat.name == "mem" {
								graphite_prefix.SendMetrics(get_mem(stat.args))
							}
							if stat.name == "swp" {
								graphite_prefix.SendMetrics(get_swp(stat.args))
							}
							if stat.name == "net" {
								graphite_prefix.SendMetrics(get_net(stat.args))
							}

						}
						send_col.Printf("%s", "Send ALL stats....")

					default:
						send_err_col.Println("Valid values -CPU, -DISK, -RAM, -SWP, -NET, -ALL")

					}
				} else {
					err_color.Printf("%s", "YOU MUST GIVE METRICS TYPE ARGUMENTS!!")
				}
			} else {
				err_color.Printf("%s", "SERVER:PORT")
			}
		} else {
				err_color.Printf("%s", "YOU MUST GIVE GRAPHITE CONNECTION SETTINGS!")
			}
		} else {
		err_color.Printf("%s", "YOU MSUT GIVE LOG PATH!")
	}

}

