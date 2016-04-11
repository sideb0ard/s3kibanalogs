package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Object_s struct {
	Key string
}

type Bucket_s struct {
	Name string
	Arn  string
}

type S3_s struct {
	S3SchemaVersion string
	Bucket          Bucket_s
	Object          Object_s
}

type Record struct {
	EventVersion string
	EventTime    string
	EventName    string
	S3           S3_s
}

type SQSMessage struct {
	Records []Record
}

type ESLogEntry struct {
	Date        string
	Time        string
	Uuid        string
	LogLocation string
	Program     string
	Facility    string
	Level       string
	Message     string
	FullLogline string
}

var q = "SQS_Q_URL"
var re = regexp.MustCompile(`.*(\d{2}:\d{2}:\d{2}).*connect: ([DIWENC]):[\d\.]+\s+T?\[([\w\s]+):\d+\]\s+(.*)`)

var rparams = &sqs.ReceiveMessageInput{
	MaxNumberOfMessages: aws.Int64(10),
	QueueUrl:            &q,
}

func setUpSQSSession() *sqs.SQS {
	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	return svc
}

func ackMessage(svc *sqs.SQS, rh string) {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      &q,
		ReceiptHandle: &rh,
	}
	fmt.Println("THOR ACK CALLED")
	resp, err := svc.DeleteMessage(params)
	if err != nil {
		fmt.Println("Ooft, while deleting message", err.Error())
		return
	} else {
		fmt.Println("THORACK:", resp)
	}
}

func getMessages(svc *sqs.SQS, jchan chan ESLogEntry) {

	resp, err := svc.ReceiveMessage(rparams)
	if err != nil {
		panic(err)
	}

	var m SQSMessage

	for _, msg := range resp.Messages {
		fmt.Println("THOR: GOT MSGs")

		err = json.Unmarshal([]byte(*msg.Body), &m)
		if err != nil {
			panic(err)
		}

		for _, r := range m.Records {
			fmt.Println("THOR: GOT RECORDS")

			orig_location := r.S3.Object.Key
			key_parts := strings.Split(r.S3.Object.Key, "/")
			uuid := key_parts[1]
			orig_date := key_parts[0]

			// download log to in memory struct
			var inmem_buffer aws.WriteAtBuffer
			downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("us-east-1")}))
			_, err := downloader.Download(&inmem_buffer,
				&s3.GetObjectInput{
					Bucket: aws.String(r.S3.Bucket.Name),
					Key:    aws.String(r.S3.Object.Key),
				})
			if err != nil {
				fmt.Println("Failed to download file", err)
				return
			}

			// uncompress gzip'd format
			var b bytes.Buffer
			b.Write(inmem_buffer.Bytes())
			uncompressed, err := gzip.NewReader(&b)
			if err != nil {
				fmt.Println(err)
			}

			// for each line, extract log entries where possible
			scanner := bufio.NewScanner(uncompressed)
			for scanner.Scan() {

				var es ESLogEntry

				if re.MatchString(scanner.Text()) {
					es.Program = "Connect"
					matches := re.FindStringSubmatch(scanner.Text())
					es.Time = matches[1]
					es.Level = matches[2]
					es.Facility = matches[3]
					es.Message = matches[4]
				} else {
					es.Program = "Kernel"
				}

				es.FullLogline = scanner.Text()
				es.LogLocation = orig_location
				es.Uuid = uuid
				es.Date = orig_date + "T" + es.Time

				// send to Elasticsearch
				jchan <- es
			}
		}
		fmt.Println("THOR DONE WITH RECORDS")
		ackMessage(svc, *msg.ReceiptHandle)
	}
}

func sendMessagesToElasticSearch(jchan chan ESLogEntry) {

	url := "ES_URL"
	client := &http.Client{}

	for {
		j := <-jchan
		fmt.Println("Ooh, got an m!")
		jlog, err := json.Marshal(j)
		buf := bytes.NewBuffer(jlog)
		if err != nil {
			fmt.Println("Errrr:", err)
		}
		os.Stdout.Write(jlog)
		fmt.Println()
		fmt.Println()

		req, err := http.NewRequest("POST", url, buf)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))

		fmt.Println()
	}
}

func main() {

	svc := setUpSQSSession()

	jchan := make(chan ESLogEntry)
	go sendMessagesToElasticSearch(jchan)

	for {
		getMessages(svc, jchan)
	}
}
