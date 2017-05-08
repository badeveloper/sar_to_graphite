package main

import ("fmt"
	"os"
	"strings"
	"bufio"
	"time"
	"log"
	"github.com/marpaia/graphite-golang"
	"os/exec"
	"bytes"
)
const ( utc_l  = "2006-01-02 15:04:05 UTC"
	graphite_server	= "localhost"
	carbon_port	= 2003
)
//Function for check exist metrics items in string and strip service title
//return metric or "Nothing"
func check_index_exist(line string, item_index int ) string {
	result_item := "Nothing"
	slice_line := strings.Split(line, ";")
	if strings.ContainsAny(slice_line[0], "#") != true {
	if len(slice_line) > 4 {
		result_item := slice_line[item_index]
		return result_item

			}
		}

		return result_item
}

func main() {

	if len(os.Args) > 1 {
		sar_log_file_path := os.Args[1]
		run_sadf := exec.Command("sadf", "-d", sar_log_file_path, "--", "-d", "-p")
		run_sadf_output, run_err := run_sadf.Output()

		if run_err != nil {
			log.Panic(run_err)
		}
		out_reader := bytes.NewReader(run_sadf_output)

		//Graphite connect
		connect_prefix, prefix_err := graphite.NewGraphite(graphite_server, carbon_port)
		if prefix_err != nil {
			log.Fatal(prefix_err)
		}
		con_err := connect_prefix.Connect()
		if con_err != nil {
			log.Fatal(con_err)
		}
		fmt.Printf("%v\n", "Send metrics to Graphite server"+":"+graphite_server)
		//Read lines from "sadf" run output
		scan_reader := bufio.NewScanner(out_reader)
		for scan_reader.Scan() {
			line := scan_reader.Text()
			timestamp_raw := check_index_exist(line, 2)

			if timestamp_raw != "Nothing" {
				timestamp, err := time.Parse(utc_l, timestamp_raw)
				if err != nil {
					log.Fatalln(err)

				}
				//Parse performance values from even string
				unix_timestamp := timestamp.Unix()
				hostname := check_index_exist(line, 0)
				dev_name := check_index_exist(line, 3)
				pts := check_index_exist(line, 4)
				rd_sec := check_index_exist(line, 5)
				wr_sec := check_index_exist(line, 6)

				//Set root graphite metric patch
				root_path := hostname + "." + "DISK_IO" + "."
				//Set Metrics
				rd_sec_metric := graphite.NewMetric(root_path+"rd_sec"+"."+dev_name, rd_sec, unix_timestamp)
				wr_sec_metric := graphite.NewMetric(root_path+"wr_sec"+"."+dev_name, wr_sec, unix_timestamp)
				pts_metric := graphite.NewMetric(root_path+"pts"+"."+dev_name, pts, unix_timestamp)
				//Collect metrics in one slice
				metric_hash := []graphite.Metric{rd_sec_metric, wr_sec_metric, pts_metric}
				//Send Metrics to Graphite
				send_err := connect_prefix.SendMetrics(metric_hash)
				if send_err != nil {
					fmt.Println(send_err)
				}

			}
		}

	} else {
	fmt.Print("No Arguments!!")
}}
