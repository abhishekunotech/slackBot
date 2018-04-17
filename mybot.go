package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/munrocape/hn/hnclient"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/websocket"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"reflect"
)

var c *hnclient.Client

type ConnInfo struct {
	ServerName   string
	ServerIP     string
	AppsInServer []string // D30, D40, Datanode Master, Datanode Slave, GTM, Postgresxl,
	SSHConn      *ssh.Client

	TimeDelta    int
	ServerIssues []ServerIss

	UlimitDetails map[string]string //  ulimit -a | awk -F '[[:space:]][[:space:]]+|) ' ' { print "\""$1","$3 }  '

	// df -h | awk -F '[[:space:][:space:]]+' ' { print "{size:"$2",\"used:\":"$3",\"available\":"$4 "}" } '
	HddDriveUtilization    []HddUtil
	CpuCount               int
	RamAvailable           int // Save the RAM in mb
	LoadAverage            []float64
	CpuAdjustedLoadAverage []float32
}
type HddUtil struct {
	Partition      string
	SpaceAllocated string
	SpaceUsed      string
	PercentageUsed string
	LastChecked    time.Time
	HasIssues      bool
}

type ServerIss struct {
	IssueType       string
	AffectedDetails string
	Message         string
	IssueCode       string
}

type responseRtmStart struct {
	Ok    bool         `json:"ok"`
	Error string       `json:"error"`
	Url   string       `json:"url"`
	Self  responseSelf `json:"self"`
}

type responseSelf struct {
	Id string `json:"id"`
}

// slackStart does a rtm.start, and returns a websocket URL and user ID. The
// websocket URL can be used to initiate an RTM session.
func slackStart(token string) (wsurl, id string, err error) {
	url := fmt.Sprintf("https://slack.com/api/rtm.start?token=%s", token)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("API request failed with code %d", resp.StatusCode)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return
	}
	var respObj responseRtmStart
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		return
	}

	if !respObj.Ok {
		err = fmt.Errorf("Slack error: %s", respObj.Error)
		return
	}

	wsurl = respObj.Url
	id = respObj.Self.Id
	return
}

// These are the messages read off and written into the websocket. Since this
// struct serves as both read and write, we include the "Id" field which is
// required only for writing.

type Message struct {
	Id      uint64 `json:"id"`
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

func getMessage(ws *websocket.Conn) (m Message, err error) {
	err = websocket.JSON.Receive(ws, &m)
	return
}

var counter uint64

func postMessage(ws *websocket.Conn, m Message) error {
	m.Id = atomic.AddUint64(&counter, 1)
	return websocket.JSON.Send(ws, m)
}

// Starts a websocket-based Real Time API session and return the websocket
// and the ID of the (bot-)user whom the token belongs to.
func slackConnect(token string) (*websocket.Conn, string) {
	wsurl, id, err := slackStart(token)
	if err != nil {
		fmt.Println(err.Error())
	}

	ws, err := websocket.Dial(wsurl, "", "https://api.slack.com/")
	if err != nil {
		fmt.Println(err.Error())
	}

	return ws, id
}

func getHardDiskResults(serverIP string) (result string) {
	result = getCommandResult("df -h | grep -v \"Filesystem\" | awk -F '[[:space:][:space:]]+' ' 0+$5 >= 70 { print $1 \"\\t\" $2 \"\\t\" $3 \"\\t\" $4 \"\\t\" $5} '", serverIP)
	return
}

func getCassandraResults(serverIP string) (result string) {
	result = getCommandResult(`/opt/cassandra/apache-cassandra-3.9/bin/nodetool status all_trade | grep -Pv "Datacenter: dc1|Status=Up/Down|State=Normal/Leaving/Joining/Moving|========|^ |Address" | awk -F '[[:space:][:space:]]+' '  { print $1 "\t" $2} '`, serverIP)
	return
}

func getPostgresxlResults(serverIP string) (result string) {
	result = getCommandResult(`pgxc_ctl monitor all | grep -i "Not Running" | wc -l`, serverIP)
	
	result = strings.Trim(result,"\n")
	fmt.Printf(`'%v'\n`,result)
	if strings.Contains(result,"0"){
		fmt.Println(result)
		fmt.Println(reflect.TypeOf(result))
		result = "Monitors are all running"
	}
	return
}

func getAWSHardDiskResults(serverIP string) (result string) {
	result = getAWSCommandResult("df -h | grep -v \"Filesystem\" | awk -F '[[:space:][:space:]]+' ' 0+$5 >= 70 { print $1 \"\\t\" $2 \"\\t\" $3 \"\\t\" $4 \"\\t\" $5} '", serverIP)
	return
}

func getAWSCommandResult(command string, serverIP string) (result string) {
	var conninfo ConnInfo
	config := &ssh.ClientConfig{
		User: "operdb",
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		Timeout: 50000,
	}
	fmt.Println("Connection Created")
	c, err := ssh.Dial("tcp", serverIP+":22", config)
	fmt.Println(c)
	if err != nil {
		fmt.Println("Error during establishing connection : ", err)

	} else {
		fmt.Println("Added ip to config", serverIP+":22", "")
		conninfo.ServerIP = serverIP + ":22"
		conninfo.ServerName = "check-server"
		conninfo.SSHConn = c

	}

	result = RunCommand(&conninfo, command)
	return
}

func getCommandResult(command string, serverIP string) (result string) {
	var conninfo ConnInfo
	config := &ssh.ClientConfig{
		User: "serveradm",
		Auth: []ssh.AuthMethod{
			ssh.Password("R3dh@t!@#"),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		Timeout: 500000000000,
	}
	//fmt.Println("Connection Created")
	c, err := ssh.Dial("tcp", serverIP+":22", config)
	fmt.Println(c)
	if err != nil {
		fmt.Println("Error during establishing connection : ", err)

	} else {
		fmt.Println("Added ip to config", serverIP+":22", "")
		conninfo.ServerIP = serverIP + ":22"
		conninfo.ServerName = "check-server"
		conninfo.SSHConn = c

	}

	result = RunCommand(&conninfo, command)
	return
}

//RunCommand is used to run any command on any  requested server
func (c *ConnInfo) RunCommand(s string) (retStr string) {
	retStr = ""
	var stdoutBuf bytes.Buffer

	var err error

	//fmt.Printf("%+v", c)

	if c.SSHConn == nil {

		fmt.Println("SSH is nil")
	} else {

		fmt.Println("SSH is not nil")
	}

	k := c.SSHConn

	if k == nil {

		fmt.Printf("K is nil %v", k)
		return
	}

		fmt.Printf("K is not nil %v", k)

	sess, err := k.NewSession()
	if err != nil {
		fmt.Println("Error in running command", err)
	} else {
				fmt.Println("Session is ",sess)
		sess.Stdout = &stdoutBuf
		start := time.Now().Nanosecond() / 1000000
		sess.Run(s)
		end := time.Now().Nanosecond() / 1000000
		fmt.Println("Run took ", (end - start), " s")
		retStr = stdoutBuf.String()
		//		fmt.Println("Stdout buffer is ",stdoutBuf)
		//		fmt.Println("Return String is ",retStr)
	}

	return
}

func RunCommand(c *ConnInfo, cmd string) string {

	return c.RunCommand(cmd)
}

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: mybot slack-bot-token\n")
		os.Exit(1)
	}

	disk_servers := []string{"10.138.32.25", "10.138.32.76", "10.138.32.77", "10.138.32.78", "10.138.32.79", "10.138.32.80", "10.138.32.81"}
	cass_servers := []string{"10.138.32.80", "10.138.32.81"}
	pgxl_servers := []string{"10.138.32.25"}

	disk_servers_iot := []string{"10.138.32.230", "10.138.32.231", "10.138.32.232", "10.138.32.212", "10.138.32.213", "10.138.32.214", "10.138.32.236", "10.138.32.229"}
	cass_servers_iot := []string{"10.138.32.230", "10.138.32.231", "10.138.32.232"}
	pgxl_servers_iot := []string{"10.138.32.236"}

	disk_servers_aws := []string{"10.63.240.31"}

	if os.Args[1] == "Local" {

	} else {

		// start a websocket-based Real Time API session
		ws, id := slackConnect(os.Args[1])
		fmt.Println("mybot ready, ^C exits")

		c = hnclient.NewClient()

		for {
			// read each incoming message
			m, err := getMessage(ws)

			if err != nil {
				log.Fatal(err)
			}
			//fmt.Println("Message Recieved : ",m.Text)
			// see if we're mentioned

			if m.Type == "message" && strings.HasPrefix(m.Text, "<@"+id+">") {
				// if so try to parse if
				//	fmt.Println("Our message found", m.Text)
				//parts := strings.Fields(m.Text)
				fmt.Println(m.Text)
				if strings.Contains(m.Text, "hard") || strings.Contains(m.Text, "Hard") || strings.Contains(m.Text, "Disk") || strings.Contains(m.Text, "disk") {
					// looks good, get the quote and reply with the result

					go func(m Message) {

						if strings.Contains(m.Text, "iot") || strings.Contains(m.Text, "IOT") || strings.Contains(m.Text, "Iot") {
							fmt.Println("Checking hard disk now!")
							for _, serverIP := range disk_servers_iot {
								m.Text = getHardDiskResults(serverIP)

								if len(m.Text) > 0 {
									m.Text = "*" + serverIP + "*" + " \n" + m.Text
									postMessage(ws, m)
								}
							}

						} else if strings.Contains(m.Text, "aws"){
							fmt.Println("Checking AWS hard disk now!")
							for _,serverIP := range disk_servers_aws{
								m.Text = getAWSCommandResult("df -h | grep -v \"Filesystem\" | awk -F '[[:space:][:space:]]+' ' 0+$5 >= 70 { print $1 \"\\t\" $2 \"\\t\" $3 \"\\t\" $4 \"\\t\" $5} '",serverIP)

								if len(m.Text) > 0{
									m.Text = "*" + serverIP + "*" + " \n" + m.Text
									postMessage(ws, m)
								}
							}
						}else {
							fmt.Println("Checking hard disk now!")
							for _, serverIP := range disk_servers {
								m.Text = getHardDiskResults(serverIP)

								if len(m.Text) > 0 {
									m.Text = "*" + serverIP + "*" + " \n" + m.Text
									postMessage(ws, m)
								}
							}
						}
					}(m)

					// NOTE: the Message object is copied, this is intentional
				} else if strings.Contains(m.Text, "hello") || strings.Contains(m.Text, "Hello") {
					m.Text = fmt.Sprintf("Hello Boss! Awaiting your orders\n")
					postMessage(ws, m)
				} else if strings.Contains(m.Text, "cass") || strings.Contains(m.Text, "Cass") {
					go func(m Message) {
						if strings.Contains(m.Text, "iot") || strings.Contains(m.Text, "IOT") {
							fmt.Println("Checking Cassandra Nodetool status!")
							for _, serverIP := range cass_servers_iot {
								m.Text = getCassandraResults(serverIP)
								m.Text = "*" + serverIP + "*" + " \n" + m.Text
								postMessage(ws, m)
							}

						} else {
							fmt.Println("Checking Cassandra Nodetool status!")
							for _, serverIP := range cass_servers {
								m.Text = getCassandraResults(serverIP)
								m.Text = "*" + serverIP + "*" + " \n" + m.Text
								postMessage(ws, m)
							}
						}
					}(m)
				} else if strings.Contains(m.Text, "postgre") || strings.Contains(m.Text, "Postgre") || strings.Contains(m.Text, "pgxl") || strings.Contains(m.Text, "Pgxl") {
					go func(m Message) {
						fmt.Println("Checking Postgresxl Monitor now!")
						if strings.Contains(m.Text, "iot") || strings.Contains(m.Text, "IOT") {
							for _, serverIP := range pgxl_servers_iot {
								m.Text = getPostgresxlResults(serverIP)
								m.Text = "*" + serverIP + "*" + " \n" + m.Text
								postMessage(ws, m)
							}

						} else {
							for _, serverIP := range pgxl_servers {
								m.Text = getPostgresxlResults(serverIP)
								m.Text = "*" + serverIP + "*" + " \n" + m.Text
								postMessage(ws, m)
							}
						}
					}(m)
				} else {
					// huh?
					m.Text = fmt.Sprintf("sorry, that does not compute\n")
					postMessage(ws, m)
				}
			}
		}
	}
}
