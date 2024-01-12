package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/gomail.v2"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const (
	QUOTE_URL1 = "https://"
	QUOTE_URL2 = "insuredapp.com/api/v1/rest/service/quote"
	QUOTE_S3   = "https://s3.console.aws.amazon.com/s3/object/insuredapp-quotes?region=us-west-1&prefix=%s/%s"
	QUOTE_IA   = "https://%s.%sinsuredapp.com/rating/requests/%s"
	PROD       = 0
	DEV        = 1
)

var (
	diffExceptions = []string{
		"<Response",
		"RqUID>",
		"<TransactionResponseDt>",
		"<TransactionEffectiveDt>",
	}
	resultRecips = []string{
		"cdinsuredappdev@zywave.com",
	}
	secretMgrKey   = "RegCheckAuthKey"
	basicAuth      = map[string]interface{}{}
	emailSecretKey = "flows/swiftmailer/prod"
	emailSrvrVals  = map[string]interface{}{}
)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, event events.S3Event) (interface{}, error) {
	if secErr := fetchFromSecretsMgr(ctx); secErr != nil {
		secMsg := fmt.Sprintf("Failed to load API auth keys and email settings from Secrets Manager: %s\n", secErr.Error())
		if secErr = notify(secMsg, resultRecips); secErr != nil {
			log.Printf("Failed to notify on error, msg: %s, new error: %s\n", secMsg, secErr.Error())
		}
		return nil, secErr
	}
	for _, record := range event.Records {
		if err := fetchFromS3(record.S3.Bucket.Name, record.S3.Object.Key); err != nil {
			msg := fmt.Sprintf("Request %s failed with error: %s\n", record.S3.Object.Key, err.Error())
			if err = notify(msg, resultRecips); err != nil {
				log.Printf("Failed to notify on error, msg: %s, new error: %s\n", msg, err.Error())
			}
		}
	}
	return nil, nil
}

func notify(msg string, to []string) error {
	var err error = nil

	iface, have := emailSrvrVals["password"]
	if !have {
		return fmt.Errorf("Failed to read email server password from secret: %s", emailSecretKey)
	}
	emailPass := iface.(string)

	emailHost := "smtp.sendgrid.net"
	if iface, have = emailSrvrVals["host"]; have {
		emailHost = iface.(string)
	}

	emailPort := 465
	if iface, have = emailSrvrVals["port"]; have {
		if portNum, errPort := strconv.Atoi(iface.(string)); errPort == nil {
			emailPort = portNum
		}
	}
	emailUser := "apikey"
	if iface, have = emailSrvrVals["username"]; have {
		emailUser = iface.(string)
	}
	emailFrom := "noreply@mail.clariondoor.com"
	if iface, have := emailSrvrVals["sender_address"]; have {
		emailFrom = iface.(string)
	}

	for _, recip := range to {
		m := gomail.NewMessage()
		m.SetHeader("From", emailFrom)
		m.SetHeader("To", recip)
		m.SetHeader("Subject", "DevTest Result")
		m.SetBody("text/plain", msg)
		d := gomail.NewDialer(emailHost, emailPort, emailUser, emailPass)
		if err = d.DialAndSend(m); err != nil {
			break
		}
	}
	return err
}

// Load the InsuredApp API Basic Auth Keys from the AWS Secrets Manager.
func fetchFromSecretsMgr(ctx context.Context) error {

	// Create Secrets Manager Session
	credFiles := []string{config.DefaultSharedCredentialsFilename()}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithSharedCredentialsFiles(credFiles))
	if err != nil {
		return err
	}
	svc := secretsmanager.New(secretsmanager.Options{
		Credentials: cfg.Credentials,
		Region:      "us-west-1",
	})

	// Fetch Secret Auth Key Table
	secOut, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &secretMgrKey,
	})
	if err != nil {
		return err
	}
	if secOut == nil {
		return fmt.Errorf("Failed to lookup secret %s", secretMgrKey)
	}

	// Parse Auth Secrets Table
	if err = json.Unmarshal([]byte(*(secOut.SecretString)), &basicAuth); err != nil {
		return err
	}

	// Fetch Secret Email Settings Table
	secOut, err = svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &emailSecretKey,
	})
	if err != nil {
		return err
	}
	if secOut == nil {
		return fmt.Errorf("Failed to lookup secret %s", emailSecretKey)
	}

	// Parse Email Secrets Table
	return json.Unmarshal([]byte(*(secOut.SecretString)), &emailSrvrVals)
}

func fetchFromS3(bucket, key string) error {

	// Create AWS Session
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1")},
	)

	// Create AWS S3 Downloader
	downloader := s3manager.NewDownloader(sess)

	// Create []byte wrapper for downloading to memory
	buf := &aws.WriteAtBuffer{}

	// Pick out the client environment from the bucket trigger
	keyPieces := strings.Split(key, "/")
	if len(keyPieces) < 2 {
		return fmt.Errorf("Invalid bucket key received: %s", key)
	}
	env := keyPieces[0]
	stripReleaseIdx := strings.Index(env, "-release-")
	if stripReleaseIdx == -1 {
		return fmt.Errorf("Test requires a release environment to trigger: %s", key)
	}
	env = env[0:stripReleaseIdx]
	auth, haveAuth := basicAuth[env]
	if !haveAuth {
		return fmt.Errorf("Env %s is not supported", env)
	}
	log.Printf("Auth key is %s\n", auth.(string))

	// Download the item from the bucket
	_, err := downloader.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return err
	}

	// Unmarshal downloaded Quote to get request, response, other fields
	quote := make(map[string]interface{})
	if err = json.Unmarshal(buf.Bytes(), &quote); err != nil {
		return err
	}
	request := ""
	requestIface, haveReq := quote["request"]
	if haveReq {
		request, haveReq = requestIface.(string)
	}
	if !haveReq {
		return fmt.Errorf("Invalid request key received: %s", key)
	}
	//log.Printf("--------------------------------------------------------\n")
	//log.Printf("Prod Request:\n%s\n", request)
	//log.Printf("--------------------------------------------------------\n")

	// Generate two quotes, one from prod and one from dev.
	var quoteResponses [2]string
	var quoteLines [2][]string
	var quoteS3Urls [2]string
	var quoteIAUrls [2]string

	devPart := ""
	for i, _ := range quoteResponses {
		reqBody := strings.NewReader(request)
		url := QUOTE_URL1 + env + "." + devPart + QUOTE_URL2
		log.Printf("Url: %s\n", url)
		req, err := http.NewRequest("POST", url, reqBody)
		if err != nil {
			return err
		}

		if err = json.Unmarshal(buf.Bytes(), &quote); err != nil {
			return err
		}
		req.Header.Add("Content-Type", "application/xml")
		req.Header.Add("Authorization", "Basic "+auth.(string))
		req.Header.Add("cache-control", "no-cache")
		req.Header.Add("X-INSUREDAPP-NOCACHE", "true")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		defer res.Body.Close()
		body, _ := ioutil.ReadAll(res.Body)

		quoteResponses[i] = string(body)
		quoteLines[i] = strings.Split(quoteResponses[i], "\n")
		quoteS3Urls[i] = ""
		quoteIAUrls[i] = ""
		qId := ""
		for _, qLine := range quoteLines[i] {
			if strings.Contains(qLine, "<RqUID>") {
				qId = strings.Replace(qLine, "<RqUID>", "", -1)
				qId = strings.Replace(qId, "</RqUID>", "", -1)
				qId = strings.Trim(qId, " ")
				if len(qId) > 0 {
					quoteS3Urls[i] = fmt.Sprintf(QUOTE_S3, env, qId)
					quoteIAUrls[i] = fmt.Sprintf(QUOTE_IA, env, devPart, qId)
				}
				break
			}
		}
		devPart = "dev."
		if quoteS3Urls[i] == "" {
			quoteS3Urls[i] = "(No Quote Successfuly Generated)"
		}
		if quoteIAUrls[i] == "" {
			quoteIAUrls[i] = "(No Quote Successfuly Generated)"
		}
	}

	linkMsg := fmt.Sprintf("Dev Quote:\n\n  S3 Link: %s\n\n  IA Link: %s\n\nProd Quote:\n\n  S3 Link: %s\n\n  IA Link: %s\n\n", quoteS3Urls[DEV], quoteIAUrls[DEV], quoteS3Urls[PROD], quoteIAUrls[PROD])
	resMsg := "The dev quote successfully matched prod!"
	if len(quoteLines[DEV]) != len(quoteLines[PROD]) {
		resMsg = fmt.Sprintf("The dev and prod quotes have a different number of lines, and should be inspected!\n"+
			"Dev lines: %d\nProd lines: %d", len(quoteLines[DEV]), len(quoteLines[PROD]))
	} else {
		diffLines := ""
		diffsLeft := 5
		prodLines := quoteLines[PROD]
		for lineIdx, devLine := range quoteLines[DEV] {
			prodLine := prodLines[lineIdx]
			if strings.Trim(devLine, " ") != strings.Trim(prodLine, " ") {
				diffOK := false
				for _, diffException := range diffExceptions {
					if strings.Contains(devLine, diffException) && strings.Contains(prodLine, diffException) {
						diffOK = true
						break
					}
				}
				if !diffOK {
					diffLines += fmt.Sprintf(" Line: %d:\n  Dev: %s\n  Prod: %s\n", lineIdx+1, devLine, prodLine)
					if diffsLeft--; diffsLeft == 0 {
						break
					}
				}
			}
		}
		if diffsLeft < 5 {
			resMsg = fmt.Sprintf("The dev and prod quotes had diffs. Up to 5 diffs listed below:\n%s\n", diffLines)
		}
	}
	msg := fmt.Sprintf("%s\n%s\n", resMsg, linkMsg)
	log.Print(msg)
	return notify(msg, resultRecips)
}
