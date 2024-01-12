package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	policyWorkers  = 3
	quoteWorkers   = 3
	portalWorkers  = 3
	maxQuoteSz     = 5000000
	errorLog       = "error.log"
	historyFile    = "prevResults.csv"
	checkFile      = "prevOutput.csv"
	resultFile     = "result.csv"
	inputFile      = "ufcicQuotes.csv"
	trnsDtFail     = "trnsDtFail.csv"
	polNumFail     = "polNumFail.csv"
	polNoTerms     = "polNoTerms.csv"
	polMissNewBiz  = "polMissNewBiz.csv"
	missEffDate    = "missEffDate.csv"
	badEffDate     = "badEffDate.csv"
	invTermDup     = "invTermDup.csv"
	invTerm1stTrns = "invTerm1stTrns.csv"
	invTermNoStart = "invTermNoStart.csv"
	invTermSubNB   = "invTermSubNB.csv"
	invTermUnBnd   = "invTermUnBnd.csv"
	invTermUnBnd2  = "invTermUnBnd2.csv"
	invUnbound1    = "invUnbound1.csv"
	invUnbound2    = "invUnbound2.csv"
	invUnbound3    = "invUnbound3.csv"
	invUnbound4    = "invUnbound4.csv"
	invUnbound5    = "invUnbound5.csv"
	cancelledTerm  = "cancelledTerm.csv"
	dupCancel      = "dupCancel.csv"
	tbdQuotes      = "tbdQuotes.csv"
	invStatus      = "invStatus.csv"
)

var (
	httpClient http.Client
	s3Client   *s3.Client
	downLoader *manager.Downloader
	upLoader   *manager.Uploader
	exclTrans  = []*transType{}

	skipExcl = map[string]bool{
		"6466799354709": true,
		"6467abc92bd49": true,
		"64691aaab287a": true,
		"655397bd05eb1": true,
	}

	fixEffDate = map[string]string{
		"645bc34d7387d": "2023-06-13",
		"6489c2233df36": "2023-07-14",
		"64c18d157e02e": "2023-09-06",
		"64c2c77b2b9b7": "2023-09-06",
	}

	fixStatus = map[string]string{
		"630d00cc8b34f": "issued",
		"6306691e10fa3": "issued",
	}
)

type checkTrans struct {
	termNumber  string
	transType   string
	transNum    string
	transStatus string
	quoteNumber string
	active      string
	inactReas   string
	allowActs   string
	convStatus  string
	failReason  string
}

type transType struct {
	portalId       string
	prevPortalId   string
	acctId         string
	acctName       string
	policyNum      string
	policyBase     string
	LOB            string
	effDate        time.Time
	origEffDate    time.Time
	expDate        time.Time
	transEffDate   time.Time
	createdEffDate time.Time
	origCrEffDate  time.Time
	endorseEffDate time.Time
	cancelEffDate  time.Time
	transStamp     time.Time
	transType      string
	transStatus    string
	origStatus     string
	lastPLS        *PlsItemType
	lastQtNum      string
	sortDate       time.Time
	notConvReas    string
}

func (t *transType) isIssued() bool {
	return (t.transStatus == "issued")
}

type transSorter []*transType

func (l transSorter) Len() int { return len(l) }
func (l transSorter) Less(i, j int) bool {

	// Order 1st by transEffDate.
	if l[i].sortDate.Year() != 1 && l[j].sortDate.Year() != 1 {
		if !l[i].sortDate.Equal(l[j].sortDate) {
			return l[i].sortDate.Before(l[j].sortDate)
		}
	}
	// Then order by transStamp.
	return l[i].transStamp.Before(l[j].transStamp)
}
func (l transSorter) Swap(i, j int) {
	tmp := l[i]
	l[i] = l[j]
	l[j] = tmp
}

type termTrans struct {
	isNewBiz     bool
	isConvPol    bool
	termEffDate  time.Time
	renewalTrans transSorter
	trans        transSorter
}

type termSorter []*termTrans

func (l termSorter) Len() int { return len(l) }
func (l termSorter) Less(i, j int) bool {
	// A term starting with New Business will always come before renewal.
	if l[i].isNewBiz || l[i].isConvPol || l[i].termEffDate.Before(l[j].termEffDate) {
		return true
	}
	return false
}

func (l termSorter) Swap(i, j int) {
	tmp := l[i]
	l[i] = l[j]
	l[j] = tmp
}

// Manages a policy or the lists of transactions that aren't policies.
type policy struct {
	policyBase       string
	allTrans         transSorter
	quotes           transSorter
	firstTermEffDate time.Time
	isConv           bool
	isParseErr       bool
	terms            termSorter
}

func fmtDt(t time.Time) string {
	if t.Year() == 1 {
		return "N/A"
	}
	return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
}

func fmtTm(t time.Time) string {
	if t.Year() == 1 {
		return "N/A"
	}
	return fmt.Sprintf(" %02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

func logToFile(fl, msg string) {
	fh, err := os.OpenFile(fl, os.O_APPEND|os.O_WRONLY, 0644)
	if os.IsNotExist(err) {
		fh, err = os.OpenFile(fl, os.O_CREATE|os.O_WRONLY, 0644)
		_ = os.Chmod(fl, 0644)
	}
	if err != nil {
		log.Fatal(fmt.Errorf("Open log file %s failed: %s", fl, err.Error()))
	}
	if _, err = fh.WriteString(msg); err == nil {
		err = fh.Close()
	}
	if err != nil {
		log.Fatal(fmt.Errorf("Write log file %s failed: %s", fl, err.Error()))
	}
}

func logPrintf(fl, strFmt string, params ...interface{}) {
	logToFile(fl, fmt.Sprintf(strFmt, params...))
}

func parseDate(dt, dtType string, rowNum int) time.Time {
	ret := time.Time{}
	if dt == "" || dt == "Invalid date" {
		return ret
	}
	delim := "/"
	if strings.Contains(dt, "-") {
		delim = "-"
	}
	dtPieces := strings.Split(dt, delim)
	if len(dtPieces) != 3 {
		//log.Fatal(fmt.Errorf("Invalid %s date %s, line %d", dtType, dt, rowNum))
		return ret
	}
	if len(dtPieces[0]) == 2 {
		dtPieces[0] = "20" + dtPieces[0]
	}
	if len(dtPieces[0]) != 4 || (dtPieces[0])[:2] != "20" {
		log.Fatal(fmt.Errorf("Invalid %s date (year) %s, line %d", dtType, dt, rowNum))
	}
	for pIdx := 1; pIdx <= 2; pIdx++ {
		if len(dtPieces[pIdx]) == 1 {
			dtPieces[pIdx] = "0" + dtPieces[pIdx]
		}
		if len(dtPieces[pIdx]) != 2 {
			log.Fatal(fmt.Errorf("Invalid %s date (day or month), %s, line %d", dtType, dt, rowNum))
		}
	}
	ret, err := time.Parse("20060102", fmt.Sprintf("%s%s%s", dtPieces[0], dtPieces[1], dtPieces[2]))
	if err != nil {
		log.Fatal(fmt.Errorf("Invalid %s date (year) %s, line %d, err: %s", dtType, dt, rowNum, err.Error()))
	}
	return ret
}

func parseTimestamp(ts string, rowNum int) time.Time {
	tsInt, err := strconv.Atoi(ts)
	if err != nil {
		log.Fatal(fmt.Errorf("Invalid timestamp %s, line %d, err: %s", ts, rowNum, err.Error()))
	}
	ret := time.Unix(int64(tsInt), 0)
	year := ret.Year()
	if year < 2010 || year > 2030 {
		log.Fatal(fmt.Errorf("Invalid timestamp %s, year %d, line %d", ts, year, rowNum))
	}
	return ret
}

func pullQuotes(transMap map[string]*policy) {
	nonPols, haveNonPols := transMap[""]
	if haveNonPols {
		for _, trans := range nonPols.allTrans {
			if trans.transType != "new business" {
				continue
			}
			nonPols.quotes = append(nonPols.quotes, trans)
		}
		if len(nonPols.quotes) > 0 {
			sort.Sort(nonPols.quotes)
		}
	}
}

func parseAllTerms(transMap map[string]*policy) transSorter {
	transMissingPolicyNum := make(transSorter, 0)
	numPols := 0
	for polNum, pol := range transMap {

		if polNum == "" {
			continue
		}

		polBadLog := ""
		polBadReas := ""
		termEffDates := make(map[string]*termTrans)

		// First, check errors, and pull in just the renewal transactions.
		for _, trans := range pol.allTrans {

			if trans.effDate.Year() == 1 && trans.notConvReas == "" {
				polBadLog = missEffDate
				polBadReas = "missing effective date"
				break
			}

			if trans.transType != "renewal" {
				continue
			}

			// Check if this is the 1st renewal for this term eff date.
			term, haveTerm := termEffDates[fmtDt(trans.effDate)]
			if haveTerm {
				// Add this renewal transaction to the existing term
				term.renewalTrans = append(term.renewalTrans, trans)
				sort.Sort(term.renewalTrans)
				continue
			}

			newTerm := &termTrans{
				isNewBiz:     false,
				isConvPol:    false,
				termEffDate:  trans.effDate,
				renewalTrans: transSorter{trans},
				trans:        transSorter{},
			}
			termEffDates[fmtDt(trans.effDate)] = newTerm
			pol.terms = append(pol.terms, newTerm)
		}

		// Next, pull in the new business transaction if it exists.
		for _, trans := range pol.allTrans {

			if trans.transType != "new business" {
				continue
			}
			_, haveTerm := termEffDates[fmtDt(trans.effDate)]
			if haveTerm || trans.effDate.After(pol.firstTermEffDate) {
				/*
					polBadLog = badEffDate
					polBadReas = fmt.Sprintf("new business must have earliest effective date for policy %s", trans.policyNum)
					break
				*/
				// Per UFCIC-1404, just make new business the earliest, lol
				trans.effDate = pol.firstTermEffDate.AddDate(0, 0, -1)
			}

			newTerm := &termTrans{
				isNewBiz:     true,
				isConvPol:    false,
				termEffDate:  trans.effDate,
				renewalTrans: transSorter{},
				trans:        transSorter{trans},
			}
			pol.terms = append(pol.terms, newTerm)
		}

		if polBadLog != "" {
			for _, trans := range pol.allTrans {
				trans.notConvReas = polBadReas
				logPrintf(polBadLog, "%s,%s,%s,%s\n", trans.portalId, trans.acctId, trans.LOB, trans.transType)
			}
			pol.isParseErr = true
			continue
		}

		// Order the terms for each policy.
		sort.Sort(pol.terms)

		// Find terms that have only a renewal in deleted/declined and a subsequent renewal terms.
		// Per UFCIC-1404, add these to the subsequent term as renewal quotes.
		//
		fixedRenewalQuotes := false
		numTerms := len(pol.terms)
		for idx, term := range pol.terms {
			if !term.isNewBiz && len(term.trans) == 0 && idx < (numTerms-1) {
				isQuoteOnly := true
				for _, renewTrans := range term.renewalTrans {
					if renewTrans.transStatus != "deleted" && renewTrans.transStatus != "declined" {
						isQuoteOnly = false
						break
					}
				}
				if isQuoteOnly {
					nextTerm := pol.terms[idx+1]
					nextTerm.renewalTrans = append(nextTerm.renewalTrans, term.renewalTrans...)
					term.renewalTrans = transSorter{}
					sort.Sort(nextTerm.renewalTrans)
					fixedRenewalQuotes = true
				}
			}
		}
		if fixedRenewalQuotes {
			newTerms := termSorter{}
			for _, chkTerm := range pol.terms {
				if len(chkTerm.trans) > 0 || len(chkTerm.renewalTrans) > 0 {
					newTerms = append(newTerms, chkTerm)
				}
			}
			pol.terms = newTerms
		}

		// On the 2nd pass, bring in all the remaining transactions, finding the appropriate term to put them in.
		for _, trans := range pol.allTrans {

			if trans.transType == "new business" || trans.transType == "renewal" {
				continue
			}

			// Find the matching term, which are sorted by effective date.
			matchingTerm := (*termTrans)(nil)
			for _, term := range pol.terms {
				if trans.effDate.Equal(term.termEffDate) || trans.effDate.After(term.termEffDate) {
					matchingTerm = term
				}
			}
			if matchingTerm == nil {
				polBadLog = invTermNoStart
				polBadReas = "sub trans no trans to start term"
				break
			}
			matchingTerm.trans = append(matchingTerm.trans, trans)
		}
		if polBadLog != "" {
			for _, trans := range pol.allTrans {
				trans.notConvReas = polBadReas
				logPrintf(polBadLog, "%s,%s,%s,%s\n", trans.portalId, trans.acctId, trans.LOB, trans.transType)
			}
			pol.isParseErr = true
		} else {
			// Order the transactions within each term.
			for _, term := range pol.terms {
				sort.Sort(term.trans)
			}
			numPols++
		}
	}
	log.Printf("Successfully loaded and ordered all terms and transactions. Num accts: %d\n", numPols)
	return transMissingPolicyNum
}

func markConvertibleTerms(transMap map[string]*policy) {
	numConv := 0
	for polNum, pol := range transMap {
		if polNum == "" || pol.isParseErr {
			continue
		}

		if len(pol.terms) == 0 {
			logPrintf(polNoTerms, "%s\n", polNum)
			failReas[polNum] = "no terms"
			continue
		}
		if !pol.terms[0].isNewBiz || len(pol.terms[0].renewalTrans) > 0 {
			pol.terms[0].isConvPol = true
		}

		//logPrintf("debugTrc.txt", "policy: %s\n", polNum)
		isConv := true
		for idx, term := range pol.terms {

			firstTrans := (*transType)(nil)

			// Check if this a renewal term, and verify only one completed renewal transaction.
			if len(term.renewalTrans) > 0 {
				firstTrans = term.renewalTrans[0]
				haveBound := false
				isComplete := false
				for _, renewTrans := range term.renewalTrans {
					if renewTrans.isIssued() {
						if haveBound {
							logPrintf(invTermDup, "%s,%s,%s\n", renewTrans.LOB, polNum, renewTrans.portalId)
							failReas[polNum] = "mult bound renewals same term"
							isConv = false
							break
						}
						haveBound = true
						if renewTrans.isIssued() {
							isComplete = true
						}
					}
				}
				if !isConv {
					break
				}
				if !isComplete {
					// Make sure incomplete term doesn't have any non-quote transactions.
					if len(term.trans) > 0 {
						logPrintf(invTermUnBnd, "%s,%s,%s\n", firstTrans.LOB, polNum, firstTrans.portalId)
						failReas[polNum] = "incomplete term with sub trans"
						isConv = false
						break
					}

					// If unbound term we cannot have future terms.
					if idx < len(pol.terms)-1 {
						logPrintf(invTermUnBnd2, "%s,%s,%s\n", firstTrans.LOB, polNum, firstTrans.portalId)
						failReas[polNum] = "incomplete term preceeds new term"
						isConv = false
						break
					}
				}
			} else {
				firstTrans = term.trans[0]
				if firstTrans.transType != "new business" {
					logPrintf(invTermNoStart, "%s,%s,%s,%s\n", firstTrans.portalId, firstTrans.acctId, firstTrans.LOB, firstTrans.transType)
					failReas[polNum] = "sub trans no trans to start term"
					isConv = false
					break
				}
				if !firstTrans.isIssued() && len(term.trans) > 1 {
					logPrintf(invTermUnBnd, "%s,%s,%s\n", firstTrans.LOB, polNum, firstTrans.portalId)
					failReas[polNum] = "incomplete term with sub trans"
					isConv = false
					break
				}
			}

			if firstTrans.transType != "new business" && firstTrans.transType != "renewal" {
				logPrintf(invTerm1stTrns, "%s,%s,%s\n", firstTrans.LOB, polNum, firstTrans.portalId)
				failReas[polNum] = "term does not start with new biz/renew"
				isConv = false
				break
			}

			//logPrintf("debugTrc.txt", "Term idx: %d, num trans: %d, term: %v, effDate: %s\n", idx, len(term.trans), *term, fmtDt(term.termEffDate))

			// Scan for non-quote transactions and handle cases that require an adjustment to satisfy PLS.
			lastTransIdx := len(term.trans) - 1

			//logPrintf("debugTrc.txt", "lastTransIdx: %d, term.trans: %v\n", lastTransIdx, term.trans)
			for tIdx, trans := range term.trans {

				//logPrintf("debugTrc.txt", "trans: %v\n", *trans)
				if trans.transType == "cancellation" || trans.transType == "endorsement" {
					nextIdx := tIdx + 1

					//logPrintf("debugTrc.txt", "nextIdx: %d\n", nextIdx)
					if nextIdx > lastTransIdx {
						haveNextTerm := (idx < len(pol.terms)-1)
						if trans.transType == "endorsement" {

							// If the term ends in an endorsement, it must be completed if there is a subsequent term.
							if !trans.isIssued() && haveNextTerm && !strings.Contains(trans.transStatus, "-delete") {
								logPrintf(invUnbound1, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
								trans.transStatus += "-delete"
							}
						} else if haveNextTerm {
							if !trans.isIssued() {
								if !strings.Contains(trans.transStatus, "-delete") {
									logPrintf(invUnbound2, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
									trans.transStatus += "-delete"
								}
							}
						}
						break
					}

					laterTrans := term.trans[nextIdx]
					if laterTrans.transType == trans.transType {

						//logPrintf("debugTrc.txt", "trans and laterTrans match, trans: %v, laterTrans: %v\n", *trans, *laterTrans)
						if !trans.isIssued() {
							if !strings.Contains(trans.transStatus, "-delete") {
								if !laterTrans.isIssued() {
									logPrintf(invUnbound3, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
								} else {
									logPrintf(invUnbound4, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
								}
								trans.transStatus += "-delete"
							}
						} else if trans.transType == "cancellation" {
							logPrintf(dupCancel, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
							failReas[polNum] = "duplicate cancel"
							isConv = false
							break
						}

					} else if !trans.isIssued() {
						if !strings.Contains(trans.transStatus, "-delete") {
							logPrintf(invUnbound5, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
							trans.transStatus += "-delete"
						}
					} else if trans.transType == "cancellation" {
						logPrintf(cancelledTerm, "%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transStatus)
						failReas[polNum] = "cancelled term"
						isConv = false
						break
					}
				}
			}

			allTermTrans := transSorter{}
			allTermTrans = append(allTermTrans, term.renewalTrans...)
			allTermTrans = append(allTermTrans, term.trans...)
			for _, trans := range allTermTrans {
				if strings.Contains(trans.transStatus, "cancel") || (trans.transType != "renewal" && strings.Contains(trans.transStatus, "declined")) {
					if trans.transType == "new business" && trans.transStatus == "cancelled" {
						log.Printf("Change cancelled to created for %s\n", trans.portalId)
						trans.transStatus = "created"
						continue
					}
					if !strings.Contains(trans.transStatus, "-delete") {
						logPrintf(invStatus, "%s,%s,%s,%s,%s\n", trans.LOB, polNum, trans.portalId, trans.transType, trans.transStatus)
						trans.transStatus += "-delete"
					}
				}
			}
			if !isConv {
				//logPrintf("debugTrc.txt", "idx %d, not simp, break\n", idx)
				break
			}
		}
		//logPrintf("debugTrc.txt", "Set policy terms from %v, isConv: %v\n", newTerms, isConv)
		if isConv {
			numConv++
		}
		pol.isConv = isConv
	}
	log.Printf("Successfully marked %d policies for conversion.\n", numConv)
}

type PlsItemType struct {
	PortalId          string   `json:"portalId"`
	RiskId            string   `json:"riskId"`
	QuoteNumber       *string  `json:"quoteNumber"`
	TransactionType   string   `json:"transactionType"`
	TermNumber        string   `json:"termNumber"`
	TransactionNumber string   `json:"transactionNumber"`
	TransactionStatus string   `json:"transactionStatus"`
	Active            bool     `json:"active"`
	InactiveReason    *string  `json:"inactiveReason"`
	AllowableActions  []string `json:"allowableActions"`
	PolicyView        bool     `json:"policyView"`
}

type RespDataType struct {
	Data []PlsItemType `json:"data"`
}

type RequestType struct {
	PortalId          *string `json:"id,omitempty"`
	LOB               *string `json:"lob,omitempty"`
	InsuredName       *string `json:"insuredName,omitempty"`
	RiskId            *string `json:"riskId,omitempty"`
	QuoteNumber       *string `json:"quoteNumber,omitempty"`
	ConvPolicyInd     *bool   `json:"convertedPolicyInd,omitempty"`
	MigratePolicyInd  *bool   `json:"migratePolicyInd,omitempty"`
	TransactionStatus *string `json:"transactionStatus,omitempty"`
	TermNumber        *string `json:"termNumber,omitempty"`
	EffDate           *string `json:"transactionEffectiveDate,omitempty"`
	QtCopyOption      *string `json:"quoteCopyOption,omitempty"`
}

func plsCall(method, cmd string, plsReq *RequestType) *RespDataType {
	reqJson, err := json.Marshal(plsReq)
	if err != nil {
		log.Fatal(fmt.Errorf("Failed to marshal PLS request data: %s", err.Error()))
	}
	//fmt.Printf("Req, cmd %s: %s\n", cmd, string(reqJson))
	apiResp := &RespDataType{}
	try := 0
	for {
		try++
		if try > 8 {
			break
		}
		if try > 1 {
			log.Printf("Sleeping, try: %d\n", try)
			time.Sleep(100 * time.Millisecond)
		}

		//req, err := http.NewRequest(method, "https://pls-dev.clariondoor.com/api/transact/"+cmd, bytes.NewBuffer(reqJson))
		req, err := http.NewRequest(method, "https://pls.ldev/api/transact/"+cmd, bytes.NewBuffer(reqJson))
		//req, err := http.NewRequest(method, "https://policy-lifecycle-service.clariondoor.com/api/transact/"+cmd, bytes.NewBuffer(reqJson))
		if err != nil {
			logPrintf(errorLog, "plsCall, NewRequest failed, try %d, req %s, err: %s\n", try, string(reqJson), err.Error())
			continue
		}
		req.Header.Set("Connection", "Keep-Alive")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-CLIENT-ID", "demoClient-guid")
		//req.Header.Set("X-CLIENT-ID", "9f466ec9-adc2-4910-a3c2-89537a8d4a2a")
		//fmt.Printf("Call http do\n")
		resp, err := httpClient.Do(req)
		if err != nil {
			logPrintf(errorLog, "plsCall, Do failed, try %d, req %s, err: %s\n", try, string(reqJson), err.Error())
			continue
		}
		//fmt.Printf("Response status: %s\n", resp.Status)
		if !strings.Contains(resp.Status, "200") {
			body2, _ := ioutil.ReadAll(resp.Body)
			logPrintf(errorLog, "plsCall, Invalid status %s, try %d, req %s, body: %s\n", resp.Status, try, string(reqJson), string(body2))
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logPrintf(errorLog, "plsCall, ReadAll failed, try %d, req %s, err: %s\n", try, string(reqJson), err.Error())
			continue
		}
		defer resp.Body.Close()
		//fmt.Printf("Resp: %s\n", string(body))

		if err = json.Unmarshal(body, apiResp); err != nil {
			logPrintf(errorLog, "plsCall, Unmarshal failed, try %d, req %s, err: %s ,body: %s\n", try, string(reqJson), err.Error(), string(body))
			continue
		}
		time.Sleep(50 * time.Millisecond)
		break
	}
	return apiResp
}

func getBaseReq(portalId, riskId, qtNum, termNum string) *RequestType {
	baseReq := &RequestType{
		PortalId: &portalId,
	}
	if termNum != "" {
		baseReq.TermNumber = &termNum
		baseReq.RiskId = &riskId
		baseReq.QuoteNumber = &qtNum
	} else {
		isTrue := true
		baseReq.ConvPolicyInd = &isTrue
		baseReq.MigratePolicyInd = &isTrue
		baseReq.LOB = &qtNum
	}
	return baseReq
}

func respFromPortalId(portalId string, allResp *RespDataType, lkp map[string]*transType) *PlsItemType {
	currResp := (*PlsItemType)(nil)
	for idx := 0; idx < len(allResp.Data); idx++ {
		resp := &allResp.Data[idx]
		trans, have := lkp[resp.PortalId]
		if !have {
			fmt.Printf("Failed to locate transaction for portalId: %s", portalId)
		}
		trans.lastPLS = resp
		if resp.QuoteNumber != nil && *resp.QuoteNumber != "" {
			trans.lastQtNum = *resp.QuoteNumber
		}
		if resp.PortalId == portalId {
			currResp = resp
		}
	}
	if currResp == nil {
		fmt.Printf("Failed to match PLS result item for portalId: %s", portalId)
		currResp = &PlsItemType{}
	}
	return currResp
}

func doNewBizTransaction(t *transType, lkp map[string]*transType) *PlsItemType {
	migratePolicy := true
	plsReq := &RequestType{
		PortalId:         &t.portalId,
		LOB:              &t.LOB,
		InsuredName:      &t.acctName,
		MigratePolicyInd: &migratePolicy,
	}
	return respFromPortalId(t.portalId, plsCall("POST", "NEW", plsReq), lkp)
}

func doCopyQtTransaction(portalId, riskId, qtNum, termNum string, lkp map[string]*transType) *PlsItemType {
	plsReq := getBaseReq(portalId, riskId, qtNum, termNum)
	cpSame := "quoteCopyToSame"
	plsReq.QtCopyOption = &cpSame
	return respFromPortalId(portalId, plsCall("POST", "COPY", plsReq), lkp)
}

func doGenericTransaction(transType, portalId, riskId, qtNum, termNum string, lkp map[string]*transType) *PlsItemType {
	plsReq := getBaseReq(portalId, riskId, qtNum, termNum)
	return respFromPortalId(portalId, plsCall("POST", transType, plsReq), lkp)
}

func doMarkTransaction(transStatus, portalId, riskId, qtNum, termNum string, transEffDate time.Time, lkp map[string]*transType) *PlsItemType {
	plsReq := getBaseReq(portalId, riskId, qtNum, termNum)
	// Can we add this here since we can't do it on new biz: MigratePolicyInd: &migratePolicy,
	plsReq.TransactionStatus = &transStatus
	if transStatus == "Completed" {
		transEffDateStr := fmt.Sprintf("%04d-%02d-%02d", transEffDate.Year(), transEffDate.Month(), transEffDate.Day())
		plsReq.EffDate = &transEffDateStr
	}
	return respFromPortalId(portalId, plsCall("POST", "MARK", plsReq), lkp)
}

type finalOutputSorter []*transType

func (l finalOutputSorter) Len() int { return len(l) }

func (l finalOutputSorter) Less(i, j int) bool {

	ii := l[i]
	jj := l[j]

	// Order 1st by policyNum.
	if ii.policyNum == "" && jj.policyNum != "" {
		return true
	} else if ii.policyNum != "" && jj.policyNum == "" {
		return false
	} else if ii.policyNum < jj.policyNum {
		return true
	} else if ii.policyNum != jj.policyNum {
		return false
	}

	// Then by effDate.
	if ii.effDate.Before(jj.effDate) {
		return true
	} else if !ii.effDate.Equal(jj.effDate) {
		return false
	}

	// Then by (computed) transEffDate (ie. sortDate).
	if ii.sortDate.Year() != 1 && jj.sortDate.Year() != 1 {
		if ii.sortDate.Before(jj.sortDate) {
			return true
		} else if !ii.sortDate.Equal(jj.sortDate) {
			return false
		}
	}

	// Finally by transStamp.
	return ii.transStamp.Before(jj.transStamp)
}

func (l finalOutputSorter) Swap(i, j int) {
	tmp := l[i]
	l[i] = l[j]
	l[j] = tmp
}

var allTransactions = finalOutputSorter{}
var allTransMap = map[string]*transType{}
var failReas = map[string]string{}
var checkMap = map[string]*checkTrans{}

func updatePortalPls(t *transType) {

	// First pull the existing quote from S3. Then we will append/overwrite the _pls property.
	quoteBuf := make([]byte, maxQuoteSz)
	at := manager.NewWriteAtBuffer(quoteBuf)
	actual, err := downLoader.Download(context.TODO(), at, &s3.GetObjectInput{
		Bucket: aws.String("quoting-portal"),
		Key:    aws.String(fmt.Sprintf("xxx/quotes-migrate/%s.json", t.portalId)),
	})
	if err != nil {
		logPrintf(errorLog, "updatePortalPls, Download failed, portalId %s, err: %s\n", t.portalId, err.Error())
		return
	}
	if actual >= maxQuoteSz {
		logPrintf(errorLog, "updatePortalPls, Quote would be truncated, portalId %s\n", t.portalId)
		return
	}
	qtData := make(map[string]interface{})
	if err = json.Unmarshal(quoteBuf[:actual], &qtData); err != nil {
		logPrintf(errorLog, "updatePortalPls, Unmarshal failed, portalId %s, err: %s\n", t.portalId, err.Error())
		return
	}
	if t.createdEffDate.Year() == 1 {
		loc, _ := time.LoadLocation("UTC")
		t.createdEffDate = time.Date(t.transStamp.Year(), t.transStamp.Month(), t.transStamp.Day(), 0, 0, 0, 0, loc)
		qtData["createdDate"] = fmtDt(t.createdEffDate)
	}

	status, have := qtData["status"].(string)
	if have && status == "bound" {
		qtData["status"] = "issued"
	}

	brokerNumber := ""
	bStr, have := qtData["brokerNumber"].(string)
	if have {
		brokerNumber = bStr
	}
	brokerNameAndNumber := ""
	bStr, have = qtData["brokerNameAndNumber"].(string)
	if have {
		brokerNameAndNumber = bStr
	}
	if brokerNumber == "" {
		_, after, haveCut := strings.Cut(brokerNameAndNumber, "(")
		if haveCut {
			before, _, haveCut2 := strings.Cut(after, ")")
			if haveCut2 {
				fmt.Printf("Change brokerNumber to %s\n", before)
				qtData["brokerNumber"] = before
			}
		}
	}

	// Append the PLS data to the quote, and marshal the data for s3 write.
	qtData["_pls"] = t.lastPLS
	qtJson, err := json.Marshal(qtData)
	if err != nil {
		logPrintf(errorLog, "updatePortalPls, Unmarshal failed, portalId %s, err: %s\n", t.portalId, err.Error())
		return
	}
	_, err = upLoader.Upload(context.TODO(), &s3.PutObjectInput{
		Body:   bytes.NewReader([]byte(qtJson)),
		Bucket: aws.String("quoting-portal"),
		Key:    aws.String(fmt.Sprintf("xxx/quotes-migrate/%s.json", t.portalId)),
	})
	if err != nil {
		logPrintf(errorLog, "updatePortalPls, Upload failed, portalId %s, err: %s\n", t.portalId, err.Error())
	}
}

func quotePlsWorker(c *chan *transType, myId int, m *sync.Mutex, numPending *int) {
	log.Printf("quotePlsWorker %d starting...\n", myId)
	for {
		t := <-*c
		if t == nil {
			break
		}
		lkpPortalIds := map[string]*transType{t.portalId: t}
		log.Printf("Process quote: %s (%d)\n", t.portalId, myId)
		resp := doNewBizTransaction(t, lkpPortalIds)
		if t.transStatus != "created" {
			resp = doMarkTransaction("Rated", t.portalId, resp.RiskId, t.lastQtNum, resp.TermNumber, t.transEffDate, lkpPortalIds)
		}
		if t.transStatus == "declined" {
			_ = doGenericTransaction("DECLINE", t.portalId, resp.RiskId, t.lastQtNum, resp.TermNumber, lkpPortalIds)
		}
		log.Printf("Quote %s completed processing\n", t.portalId)
		m.Lock()
		*numPending -= 1
		m.Unlock()
	}
	log.Printf("quotePlsWorker %d finished!\n", myId)
}

func policyPlsWorker(c *chan *policy, myId int, m *sync.Mutex, numPending *int) {
	log.Printf("policyPlsWorker %d starting...\n", myId)
	for {
		pol := <-*c
		if pol == nil {
			break
		}
		polNum := pol.policyBase
		log.Printf("Process policy: %s (%d)\n", polNum, myId)
		riskId := ""
		resp := (*PlsItemType)(nil)
		lkpPortalIds := make(map[string]*transType)
		subTrans := make(transSorter, 0)
		for _, term := range pol.terms {

			//fmt.Printf("term: %v, resp: %v, polNum: %v\n", term, resp, polNum)
			// Handle all of the renewal quotes at once.
			if len(term.renewalTrans) > 0 {
				// In the 1st pass, just create the quotes.
				lastQtNum := ""
				for rIdx, renewTrans := range term.renewalTrans {
					//fmt.Printf("lastQtNum: %v, renewTrans: %v\n", lastQtNum, renewTrans)
					lkpPortalIds[renewTrans.portalId] = renewTrans
					if rIdx == 0 {
						fakeQtNum := ""
						termNum := ""
						if term.isConvPol {
							fakeQtNum = renewTrans.LOB
						} else {
							termNum = resp.TermNumber
						}
						//fmt.Printf("portal: %s, riskId: %s\n", renewTrans.portalId, riskId)
						//fmt.Printf("fakeQtNum: %s, termNum: %s\n", fakeQtNum, termNum)
						resp = doGenericTransaction("RENEW", renewTrans.portalId, riskId, fakeQtNum, termNum, lkpPortalIds)
						riskId = resp.RiskId
						if resp.QuoteNumber != nil {
							lastQtNum = *resp.QuoteNumber
						}
					} else {
						resp = doCopyQtTransaction(renewTrans.portalId, riskId, lastQtNum, resp.TermNumber, lkpPortalIds)
					}
				}

				// In the 2nd pass, handle declined and expired quotes, or else rate it.
				for _, renewTrans := range term.renewalTrans {
					if renewTrans.transStatus == "declined" {
						resp = doGenericTransaction("DECLINE", renewTrans.portalId, riskId, renewTrans.lastQtNum, resp.TermNumber, lkpPortalIds)
					} else if renewTrans.transStatus != "created" {
						resp = doMarkTransaction("Rated", renewTrans.portalId, riskId, renewTrans.lastQtNum, resp.TermNumber, renewTrans.transEffDate, lkpPortalIds)
					}
				}

				// In the final pass, bind a single renewal, if applicable.
				for _, renewTrans := range term.renewalTrans {
					if renewTrans.isIssued() {
						resp = doMarkTransaction("Bound", renewTrans.portalId, riskId, renewTrans.lastQtNum, resp.TermNumber, renewTrans.transEffDate, lkpPortalIds)
						resp = doMarkTransaction("Completed", renewTrans.portalId, riskId, renewTrans.lastQtNum, resp.TermNumber, renewTrans.transEffDate, lkpPortalIds)
						break
					}
				}
			}

			for _, trans := range term.trans {
				if strings.Contains(trans.transStatus, "-delete") {
					trans.lastPLS = nil
					continue
				}
				lkpPortalIds[trans.portalId] = trans
				if trans.transType != "new business" {
					subTrans = append(subTrans, trans)
					continue
				}
				resp = doNewBizTransaction(trans, lkpPortalIds)
				riskId = resp.RiskId
				if trans.transStatus != "created" {
					resp = doMarkTransaction("Rated", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
				}
				if trans.isIssued() {
					resp = doMarkTransaction("Bound", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
					resp = doMarkTransaction("Completed", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
				}
			}
		}
		sort.Sort(subTrans)
		for _, trans := range subTrans {

			// Match this subsequent transaction to its correct PLS data.
			for _, term := range pol.terms {
				nbTrans := (*transType)(nil)
				for _, t := range term.trans {
					if t.transType == "new business" {
						nbTrans = t
					} else if trans.portalId == t.portalId && nbTrans != nil {
						trans.lastPLS = nbTrans.lastPLS
						trans.lastQtNum = nbTrans.lastQtNum
						break
					}
				}

				if trans.lastPLS == nil {
					var lastMatchStamp time.Time
					lastRenew := (*transType)(nil)
					for _, renewTrans := range term.renewalTrans {
						if renewTrans.isIssued() {
							lastRenew = renewTrans
							break
						} else if renewTrans.transStamp.After(lastMatchStamp) {
							lastRenew = renewTrans
							lastMatchStamp = renewTrans.transStamp
						}
					}
					if lastRenew != nil {
						for _, t := range term.trans {
							if trans.portalId == t.portalId {
								trans.lastPLS = lastRenew.lastPLS
								trans.lastQtNum = lastRenew.lastQtNum
								break
							}
						}
					}
				}
			}
			resp = trans.lastPLS
			if resp == nil {
				log.Fatal(fmt.Errorf("Subsequent portal %s has no prev PLS data", trans.portalId))
			}

			doDelete := false
			if strings.Contains(trans.transStatus, "-delete") {
				doDelete = true
				trans.transStatus = strings.Replace(trans.transStatus, "-delete", "", -1)
			}
			switch trans.transType {
			case "endorsement":
				resp = doGenericTransaction("ENDORSE", trans.portalId, riskId, "", resp.TermNumber, lkpPortalIds)
			case "cancellation":
				resp = doGenericTransaction("CANCEL", trans.portalId, riskId, "", resp.TermNumber, lkpPortalIds)
			default:
				log.Fatal(fmt.Errorf("Can't do trans type %s for simple term", trans.transType))
			}
			if doDelete {
				resp = doGenericTransaction("DELETE", trans.portalId, resp.RiskId, trans.lastQtNum, resp.TermNumber, lkpPortalIds)
			} else {
				if trans.transStatus != "created" {
					resp = doMarkTransaction("Rated", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
				}
				if trans.isIssued() {
					resp = doMarkTransaction("Bound", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
					resp = doMarkTransaction("Completed", trans.portalId, riskId, trans.lastQtNum, resp.TermNumber, trans.transEffDate, lkpPortalIds)
				}
			}
		}
		log.Printf("Policy %s completed processing\n", polNum)
		m.Lock()
		*numPending -= 1
		m.Unlock()
	}
	log.Printf("policyPlsWorker %d finished!\n", myId)
}

func portalS3Worker(c *chan *transType, myId int, m *sync.Mutex, numPending *int) {
	log.Printf("portalS3Worker %d starting...\n", myId)
	for {
		t := <-*c
		if t == nil {
			break
		}
		pls := t.lastPLS
		chkTrans, doChk := checkMap[t.portalId]
		conv := "conv"
		active := "N/A"
		inactiveReason := ""
		misMatch := ""

		if pls == nil {
			pls = &PlsItemType{}
			conv = "not conv"
			possReas, have := failReas[t.policyBase]
			if have {
				t.notConvReas = possReas
			}
			if t.notConvReas == "" {
				t.notConvReas = "???"
			}
			if doChk {
				if chkTrans.convStatus != conv {
					misMatch += fmt.Sprintf(", conv, was %s, now %s", chkTrans.convStatus, conv)
				} else if chkTrans.failReason != t.notConvReas {
					misMatch += fmt.Sprintf(", fail reason, was %s, now %s", chkTrans.failReason, t.notConvReas)
				}
			}
		} else {
			active = fmt.Sprintf("%v", pls.Active)
			if pls.InactiveReason != nil {
				inactiveReason = *pls.InactiveReason
			}
			pls.PolicyView = pls.Active && pls.TransactionType != "Deletion"
			if !pls.PolicyView {
				if pls.TransactionType == "New Business" || pls.TransactionType == "Renewal" {
					if inactiveReason == "Declined" || inactiveReason == "Lost" || inactiveReason == "Expired" {
						pls.PolicyView = true
					}
				}
			}
			//log.Printf("Process portal ID: %s (%d)\n", t.portalId, myId)
			//updatePortalPls(t)
			//log.Printf("Portal ID %s completed processing\n", t.portalId)

			if doChk && chkTrans.convStatus != conv {
				misMatch += fmt.Sprintf(", conv, was %s/%s, now %s", chkTrans.convStatus, chkTrans.failReason, conv)
			}
		}

		if doChk && conv == "conv" {
			if pls.TermNumber != chkTrans.termNumber {
				misMatch += fmt.Sprintf(", termNumber, was %s, now %s", chkTrans.termNumber, pls.TermNumber)
			}
			if pls.TransactionType != chkTrans.transType {
				misMatch += fmt.Sprintf(", transType, was %s, now %s", chkTrans.transType, pls.TransactionType)
			}
			if pls.TransactionNumber != chkTrans.transNum {
				misMatch += fmt.Sprintf(", transNum, was %s, now %s", chkTrans.transNum, pls.TransactionNumber)
			}
			if pls.TransactionStatus != chkTrans.transStatus {
				misMatch += fmt.Sprintf(", transStatus, was %s, now %s", chkTrans.transStatus, pls.TransactionStatus)
			}
			qtNumChk := ""
			qtNumPieces := strings.Split(t.lastQtNum, "-")
			if len(qtNumPieces) == 2 {
				qtNumChk = qtNumPieces[1]
			}
			if qtNumChk != chkTrans.quoteNumber {
				misMatch += fmt.Sprintf(", quoteNumber, was %s, now %s", chkTrans.quoteNumber, qtNumChk)
			}
			if active != chkTrans.active {
				misMatch += fmt.Sprintf(", active, was %s, now %s", chkTrans.active, active)
			}
			if inactiveReason != chkTrans.inactReas {
				misMatch += fmt.Sprintf(", inactReas, was %s, now %s", chkTrans.inactReas, inactiveReason)
			}
			chkAllow := []string{}
			if chkTrans.allowActs != "" {
				chkAllow = strings.Split(chkTrans.allowActs, ",")
			}
			aaMM := false
			if len(chkAllow) != len(pls.AllowableActions) {
				aaMM = true
			} else {
				toMatch := len(chkAllow)
				for _, aaChk := range chkAllow {
					for _, aa := range pls.AllowableActions {
						if aaChk == aa {
							toMatch--
							break
						}
					}
				}
				if toMatch != 0 {
					aaMM = true
				}
			}
			if aaMM {
				misMatch += fmt.Sprintf(", allowActs, was %v, now %v", chkAllow, pls.AllowableActions)
			}
		}
		if misMatch != "" {
			logPrintf("misMatch.log", "Portal: %s%s\n", t.portalId, misMatch)
		}

		acctNm := strings.Replace(t.acctName, `"`, `""`, -1)

		m.Lock()
		logPrintf(resultFile, `%s,%s,%s,%s,%s,%s,%s,%v,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s,%s,%v,%s`+"\n", t.portalId, pls.RiskId, pls.TermNumber, pls.TransactionType,
			pls.TransactionNumber, pls.TransactionStatus, t.lastQtNum, active, inactiveReason, t.LOB, t.acctId, acctNm, t.policyNum, t.transType, t.origStatus, t.prevPortalId, fmtDt(t.origEffDate),
			fmtDt(t.expDate), fmtDt(t.sortDate), fmtDt(t.transEffDate), fmtDt(t.createdEffDate), fmtDt(t.origCrEffDate), fmtDt(t.endorseEffDate), fmtDt(t.cancelEffDate),
			fmtDt(t.transStamp)+fmtTm(t.transStamp), strings.Join(pls.AllowableActions, ","), conv, t.notConvReas, pls.PolicyView, "no")

		*numPending -= 1
		m.Unlock()
	}
	if myId == 1 {
		//fmt.Printf("exclTrans2: %v\n", exclTrans)
		for _, et := range exclTrans {
			m.Lock()
			acctNm := strings.Replace(et.acctName, `"`, `""`, -1)
			logPrintf(resultFile, `%s,,,,,,,,,%s,%s,"%s",%s,%s,%s,%s,%s,%s,,%s,%s,%s,%s,%s,%s,,%s,%s,,%s`+"\n",
				et.portalId, et.LOB, et.acctId, acctNm, et.policyNum, et.transType, et.transStatus, et.prevPortalId, fmtDt(et.effDate),
				fmtDt(et.expDate), fmtDt(et.transEffDate), fmtDt(et.createdEffDate), fmtDt(et.origCrEffDate), fmtDt(et.endorseEffDate), fmtDt(et.cancelEffDate),
				fmtDt(et.transStamp)+fmtTm(et.transStamp), "not conv", "excluded", "yes")
			m.Unlock()
		}
	}
	log.Printf("portalS3Worker %d finished!\n", myId)
}

func main() {
	dialCtx := net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
	httpTrans := http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&dialCtx).DialContext,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	}
	httpClient = http.Client{
		Transport: &httpTrans,
		Timeout:   time.Duration(30 * time.Second),
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatal(fmt.Errorf("unable to load AWS SDK config: %s", err.Error()))
	}
	s3Client = s3.NewFromConfig(cfg)
	downLoader = manager.NewDownloader(s3Client)
	upLoader = manager.NewUploader(s3Client)

	// Cache all PortalId's previously loaded or errored out.
	histMap := make(map[string]bool)
	cacheHist := true
	hist, err := os.Open(historyFile)
	if os.IsNotExist(err) {
		cacheHist = false
	} else if err != nil {
		log.Fatal(fmt.Errorf("Error opening %s for Portal ID's already loaded: %s", historyFile, err.Error))
	}
	if cacheHist {
		log.Printf("Caching all Portal ID's that have already been processed...\n")
		histCsv := bufio.NewScanner(hist)
		for histCsv.Scan() {
			line := histCsv.Text()
			if len(line) > 0 {
				flds := strings.Split(line, ",")
				if len(flds) > 0 {
					histMap[flds[0]] = true
				}
			}
		}
		hist.Close()
	}

	// Cache non-quote PortalIds to be diffed against for regression test.
	cacheChecks := true
	chk, err := os.Open(checkFile)
	if os.IsNotExist(err) {
		cacheChecks = false
	} else if err != nil {
		log.Fatal(fmt.Errorf("Error opening %s for regression checks: %s", checkFile, err.Error()))
	}
	if cacheChecks {
		log.Printf("Caching Portal IDs for regression check...\n")
		chkCsv := csv.NewReader(chk)
		for {
			chkRec, err := chkCsv.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			if len(chkRec) < 28 || chkRec[0] == "" {
				log.Printf("Not enough fields in regression check file.\n")
				continue
			}
			chkTrans := &checkTrans{
				termNumber:  chkRec[2],
				transType:   chkRec[3],
				transNum:    chkRec[4],
				transStatus: chkRec[5],
				quoteNumber: "",
				active:      chkRec[7],
				inactReas:   chkRec[8],
				allowActs:   chkRec[25],
				convStatus:  chkRec[26],
				failReason:  chkRec[27],
			}

			if chkTrans.convStatus != "not conv" {
				qtNumPieces := strings.Split(chkRec[6], "-")
				if len(qtNumPieces) == 2 {
					chkTrans.quoteNumber = qtNumPieces[1]
				}
			}
			checkMap[chkRec[0]] = chkTrans
		}
		chk.Close()
	}

	// Load the input transactions by policy, skipping anything already loaded.
	log.Printf("Loading input transactions and grouping by policy if applicable...\n")
	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(fmt.Errorf("Failed to open input transactions file %s: %s", inputFile, err.Error()))
	}
	r := csv.NewReader(f)
	transMap := make(map[string]*policy)
	numAlreadyLoaded := 0
	hdrSkipped := false
	rowNum := 2
	numLoaded := 0
	dupKey := 1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if !hdrSkipped {
			hdrSkipped = true
			continue
		}
		if len(record) < 16 || record[0] == "" {
			log.Printf("Not enough fields, skipping line: %d ...\n", rowNum)
			rowNum++
			continue
		}
		_, alreadyLoaded := histMap[record[0]]
		if alreadyLoaded {
			numAlreadyLoaded++
			continue
		}

		//www := record[4]    // transType
		//xxx := record[9]   // acctId
		yyy := record[0] // portalId
		/*
		   		zzz := record[3]    // policyNum
		           if !strings.Contains(zzz, "01-CGL-100943-") {
		   			continue;
		   		}
		*/

		if yyy == "633f10ca8fdb7" {
			continue
		}
		/*
			if zzz != "URE200585" && zzz != "UPO100214" && zzz != "UPO100236" &&
				zzz != "UPO102908" && zzz != "URE200457" && zzz != "URE200636" &&
				zzz != "URE200643" {
				continue
			}
		*/

		trans := &transType{
			portalId:     record[0],
			prevPortalId: record[7],
			acctId:       record[9],
			acctName:     record[15],
			policyNum:    record[3],
			LOB:          record[8],
			transType:    record[4],
			transStatus:  record[6],
			lastPLS:      nil,
		}

		trans.origStatus = trans.transStatus
		trans.effDate = parseDate(record[1], "effective", rowNum)
		trans.origEffDate = trans.effDate
		trans.expDate = parseDate(record[2], "expiration", rowNum)
		trans.transEffDate = parseDate(record[5], "transaction effective", rowNum)
		trans.createdEffDate = parseDate(record[10], "created effective", rowNum)
		trans.origCrEffDate = parseDate(record[11], "orig created effective", rowNum)
		trans.endorseEffDate = parseDate(record[12], "endorsement effective", rowNum)
		trans.cancelEffDate = parseDate(record[13], "cancellation effective", rowNum)
		trans.transStamp = parseTimestamp(record[14], rowNum)
		trans.sortDate = trans.transEffDate
		if trans.transStatus == "bound" {
			trans.transStatus = "issued"
		}
		if yyy == "6240d49053daf" {
			trans.transStatus = "issued"
		}

		_, isExcl := skipExcl[yyy]
		if isExcl {
			exclTrans = append(exclTrans, trans)
			continue
		}

		useEffDate, haveFixDate := fixEffDate[yyy]
		if haveFixDate {
			trans.effDate = parseDate(useEffDate, "effective", rowNum)
		}

		useStatus, haveFixStatus := fixStatus[yyy]
		if haveFixStatus {
			trans.transStatus = useStatus
		}

		polPieces := strings.Split(trans.policyNum, "-")
		if len(polPieces) > 0 {
			trans.policyBase = strings.Join(polPieces[:len(polPieces)-1], "-")
		} else {
			trans.policyBase = trans.policyNum
		}

		if trans.sortDate.Year() == 1 && trans.transStatus == "bound" {
			if trans.transType == "new business" {
				trans.sortDate = trans.effDate
			} else if trans.transType == "endorsement" || trans.transType == "cancellation" {
				if trans.transType == "endorsement" {
					trans.sortDate = trans.endorseEffDate
				} else {
					trans.sortDate = trans.cancelEffDate
				}
				if trans.sortDate.Year() == 1 {
					logPrintf(trnsDtFail, "%s,%s,%s\n", trans.portalId, trans.acctId, trans.transType)
				}
			}
		}

		allTransactions = append(allTransactions, trans)
		allTransMap[trans.portalId] = trans

		polBase := trans.policyBase
		pol, have := transMap[polBase]

		// Check for dup new business transactions. See SD-4904, policy 01-100013-01
		if have && polBase != "" && trans.transType == "new business" {
			for _, t := range pol.allTrans {
				if t.transType == "new business" {
					polBase = fmt.Sprintf("dup%d-%s\n", dupKey, polBase)
					fmt.Printf("Created polBase: %s\n", polBase)
					dupKey++
					break
				}
			}
		}
		if !have || polBase != trans.policyBase {
			pol = &policy{
				policyBase:       trans.policyBase,
				allTrans:         transSorter{},
				quotes:           transSorter{},
				isConv:           false,
				isParseErr:       false,
				firstTermEffDate: trans.effDate,
				terms:            termSorter{},
			}
			transMap[polBase] = pol
		} else if (trans.transType == "new business" || trans.transType == "renewal") && trans.effDate.Before(pol.firstTermEffDate) {
			pol.firstTermEffDate = trans.effDate
		}
		pol.allTrans = append(pol.allTrans, trans)
		numLoaded++
		rowNum++
	}
	log.Printf("Successfully loaded %d transactions for %d policies, skipped %d.\n", numLoaded, len(transMap), numAlreadyLoaded)

	log.Printf("Parse all non-quote transactions into terms...\n")
	missingPolicyNumber := parseAllTerms(transMap)
	for _, trans := range missingPolicyNumber {
		trans.notConvReas = "missing policy number"
		logPrintf(polNumFail, "%s,%s,%s,%s,%s\n", trans.portalId, trans.acctId, trans.LOB, trans.transType, fmtDt(trans.effDate))
	}

	log.Printf("Starting PLS migration...\n")
	logPrintf(resultFile, "Portal.PortalId,PLS.RiskId,PLS.TermNumber,PLS.TransactionType,PLS.TransactionNumber,PLS.TransactionStatus,PLS.QuoteNumber,PLS.Active,PLS.InactiveReason,Portal.LOB,Portal.PortalAcctId,Portal.InsuredName,Portal.PolicyNumber,Portal.TransType,Portal.PortalStatus,Portal.PrevPortalId,Portal.EffDate,Portal.ExpDate,Corrected TransEffDate,Portal.TransEffDate,Portal.CreatedDate,Portal.OrigCreatedDate,Portal.EndorseEffDate,Portal.CancelEffDate,Portal.Timestamp,PLS.AllowableActions,Conv Status,Fail Reason,ViewPolicy,Excluded\n")

	workMux := &sync.Mutex{}
	numPending := 0
	pullQuotes(transMap)
	nonPols, haveNonPols := transMap[""]
	if haveNonPols && len(nonPols.quotes) > 0 {
		quoteChan := make(chan *transType)
		for qtPlsId := 1; qtPlsId <= quoteWorkers; qtPlsId++ {
			go quotePlsWorker(&quoteChan, qtPlsId, workMux, &numPending)
		}
		for _, t := range nonPols.quotes {
			workMux.Lock()
			numPending++
			workMux.Unlock()
			quoteChan <- t
		}
		for qtPlsId := 1; qtPlsId <= quoteWorkers; qtPlsId++ {
			quoteChan <- nil
		}
		for {
			log.Printf("Checking for pending quote workers...\n")
			workMux.Lock()
			remaining := numPending
			workMux.Unlock()
			if remaining <= 0 {
				log.Printf("Successfully created %d quote transactions\n", len(nonPols.quotes))
				break
			}
			log.Printf("Waiting for %d pending quotes to complete\n", remaining)
		}
	}

	log.Printf("Mark all the convertible terms...\n")
	markConvertibleTerms(transMap)

	numConv := 0
	numPending = 0
	policyChan := make(chan *policy)
	for polPlsId := 1; polPlsId <= policyWorkers; polPlsId++ {
		go policyPlsWorker(&policyChan, polPlsId, workMux, &numPending)
	}
	for _, pol := range transMap {
		if !pol.isConv {
			continue
		}
		workMux.Lock()
		numPending++
		workMux.Unlock()
		policyChan <- pol
		numConv++
	}
	for polPlsId := 1; polPlsId <= policyWorkers; polPlsId++ {
		policyChan <- nil
	}
	for {
		log.Printf("Checking for pending policy workers...\n")
		workMux.Lock()
		remaining := numPending
		workMux.Unlock()
		if remaining <= 0 {
			log.Printf("Successfully created all the convertible terms, num polices: %d.\n", numConv)
			break
		}
		log.Printf("Waiting for %d pending policies to complete\n", remaining)
	}

	sort.Sort(allTransactions)
	portalChan := make(chan *transType)
	numPending = 0
	for pWrkrId := 1; pWrkrId <= portalWorkers; pWrkrId++ {
		go portalS3Worker(&portalChan, pWrkrId, workMux, &numPending)
	}
	for _, t := range allTransactions {

		if strings.Contains(t.transStatus, "-delete") {
			continue
		}
		workMux.Lock()
		numPending++
		workMux.Unlock()
		portalChan <- t
	}
	for pWrkrId := 1; pWrkrId <= portalWorkers; pWrkrId++ {
		portalChan <- nil
	}
	for {
		log.Printf("Checking for pending portal workers...\n")
		workMux.Lock()
		remaining := numPending
		workMux.Unlock()
		if remaining <= 0 {
			break
		}
		log.Printf("Waiting for %d pending portal S3 updates to complete\n", remaining)
	}
	time.Sleep(5 * time.Second)
}
