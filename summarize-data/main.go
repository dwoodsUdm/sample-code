package main

import (
	"cloud.google.com/go/bigquery"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"os"
	"time"
	"udm/logClnt"
	rc "udm/radConfig"
	uu "udm/utils"
)

const (
	stageSummaryQry = `
        SELECT
          hour,
          reqType,
          prodType,
          prebidSiteId,
          prebidSiteName,
          juiceType,
          sid,
          CASE
            WHEN impMid > 0 THEN impMid
            WHEN evMid > 0 THEN evMid
            ELSE bidMid
          END mid,
          IF(advName = "N/A" OR advName = "Unknown", "", advName) advName,
          adUnit,
          IF(r.adType = "", IFNULL(m.adType, ""), r.adType) adType,
          ctry,
          state,
          dma,
          CASE
            WHEN device = '0' THEN 'Desktop'
            WHEN device = 'ipad' OR device LIKE '%%tablet%%' THEN 'Tablet'
            WHEN device = 'iphone' OR device LIKE '%%mobile%%' THEN 'Mobile'
            ELSE 'Other'
          END device,
          CASE
            WHEN LOWER(browser) LIKE '%%android%%' THEN 'Android'
            WHEN LOWER(browser) LIKE '%%aolshield%%' THEN 'AOLShield'
            WHEN LOWER(browser) LIKE '%%chrome%%' THEN 'Chrome'
            WHEN LOWER(browser) LIKE '%%chromium%%' THEN 'Chromium'
            WHEN LOWER(browser) = 'edge' THEN 'Edge'
            WHEN LOWER(browser) LIKE '%%facebook%%' THEN 'Facebook'
            WHEN LOWER(browser) LIKE '%%firefox%%' THEN 'Firefox'
            WHEN LOWER(browser) LIKE '%%goanna%%' THEN 'Goanna'
            WHEN LOWER(browser) LIKE '%%internet explorer%%' THEN 'Internet Explorer'
            WHEN LOWER(browser) LIKE '%%mozilla%%' THEN 'Mozilla'
            WHEN LOWER(browser) = 'news' THEN 'News'
            WHEN LOWER(browser) LIKE '%%opera%%' THEN 'Opera'
            WHEN LOWER(browser) LIKE '%%safari%%' THEN 'Safari'
            WHEN LOWER(browser) LIKE '%%snapchat%%' THEN 'Snapchat'
            ELSE 'Other'
          END browser,
          CASE
            WHEN LOWER(os) LIKE '%%android%%' THEN 'Android'
            WHEN LOWER(os) LIKE '%%cros%%' THEN 'CrOS'
            WHEN LOWER(os) LIKE '%%fedora%%' THEN 'Fedora'
            WHEN LOWER(os) LIKE '%%ios%%' THEN 'iOS'
            WHEN LOWER(os) LIKE '%%linux%%' THEN 'Linux'
            WHEN LOWER(os) LIKE '%%mac os%%' THEN 'Mac OS'
            WHEN LOWER(os) LIKE '%%opera%%' THEN 'Opera'
            WHEN LOWER(os) LIKE '%%ubuntu%%' THEN 'Ubuntu'
            WHEN LOWER(os) LIKE '%%windows%%' THEN 'Windows'
            ELSE 'Other'
          END os,
          domain,
          clientVersion,
          utmCampaign,
          utmMedium,
          utmSource,
          utmTerm,
          utmContent,
          subId,
          edgeStyle,
          IFNULL(COUNT(DISTINCT IF(advName = "N/A" AND refReqCode = 0,
                                                   reqId, NULL)), 0) impRequests,
          IFNULL(COUNT(DISTINCT IF(advName = "N/A" AND refReqCode > 0,
                                                   reqId, NULL)), 0) refRequests,
          IFNULL(CAST(ROUND(SUM(auctions *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) auctions,
          IFNULL(CAST(ROUND(SUM(auSzReqs *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) bidRequests,
          IFNULL(CAST(ROUND(SUM(auSzBids *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) bids,
          IFNULL(SUM(auSzBidAmt * IF(radPercent = 0, 0, 100/radPercent)), 0) bidAmt,
          IFNULL(CAST(ROUND(SUM(auSzWins *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) wonBids,
          IFNULL(SUM(auSzWonAmt * IF(radPercent = 0, 0, 100/radPercent)), 0) wonBidAmt,
          IFNULL(SUM(paidImps), 0) paidImps,
          IFNULL(SUM(IF(refNotifyCode > 0, paidImps, 0)), 0) refImps,
          IFNULL(SUM(defImps), 0) defImps,
          IFNULL(SUM(sessions), 0) sessions,
          IFNULL(SUM(rev + eRev), 0) rev,
          IFNULL(SUM(pubRev + ePubRev), 0) pubRev,
          IFNULL(CAST(ROUND(SUM(timedOut *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) timeOuts,
          IFNULL(CAST(ROUND(SUM(took *
               IF(radPercent = 0, 0, 100/radPercent))) AS INT64), 0) respTook,
          IFNULL(SUM(clicks), 0) clicks
        FROM
          (
            SELECT DISTINCT
              reqId,
              hour,
              reqType,
              prodType,
              prebidSiteId,
              prebidSiteName,
              juiceType,
              sid,
              bidder advName,
              IFNULL(impMid, 0) impMid,
              impNum,
              IFNULL(evMid, 0) evMid,
              evNum,
              bidMid,
              IF(rev > 0, FIRST_VALUE(adUnitId) OVER (PARTITION BY reqId
                ORDER BY IF(auSz IS NOT NULL AND won, 0, 1)), adUnitId) adUnit,
              IFNULL(auSz, '') adType,
              IFNULL(refReqCode, 0) refReqCode,
              IFNULL(refNotifyCode, 0) refNotifyCode,
              IFNULL(ctry, '') ctry,
              IFNULL(state, '') state,
              IFNULL(dma, '') dma,
              IFNULL(domain, '') domain,
              IFNULL(device, '') device,
              IFNULL(browser, '') browser,
              IFNULL(os, '') os,
              IFNULL(clientVers, '') clientVersion,
              IFNULL(utmCampaign, '') utmCampaign,
              IFNULL(utmMedium, '') utmMedium,
              IFNULL(utmSource, '') utmSource,
              IFNULL(utmTerm, '') utmTerm,
              IFNULL(utmContent, '') utmContent,
              IFNULL(subId, '') subId,
              IFNULL(style, '') edgeStyle,
              radPercent,
              IF(auSz IS NOT NULL AND auSz = FIRST_VALUE(IF(auSz IS NOT NULL, auSz, NULL)) OVER 
                     (PARTITION BY reqId, bidder ORDER BY auSz), 1, 0) auctions,
              IF(auSz IS NOT NULL, took, 0) took,
              IF(auSz IS NOT NULL, timedOut, 0) timedOut,
              IF(impNum <= 0, SUM(IF(impNum <= 0 AND auSz IS NOT NULL, 1, 0))
                OVER (PARTITION BY reqId, adUnitId, auSz, bidder), 0) auSzReqs,
              IF(impNum <= 0, SUM(IF(impNum <= 0 AND auSz IS NOT NULL AND cpm > 0, 1, 0))
                OVER (PARTITION BY reqId, adUnitId, auSz, bidder), 0) auSzBids,
              IF(impNum <= 0, SUM(IF(impNum <= 0 AND auSz IS NOT NULL, cpm, 0))
                OVER (PARTITION BY reqId, adUnitId, auSz, bidder), 0) auSzBidAmt,
              IF(impNum <= 0, SUM(IF(impNum <= 0 AND auSz IS NOT NULL AND won, 1, 0))
                OVER (PARTITION BY reqId, bidder, adUnitId, auSz), 0) auSzWins,
              IF(impNum <= 0, SUM(IF(impNum <= 0 AND auSz IS NOT NULL AND won, cpm, 0))
                OVER (PARTITION BY reqId, bidder, adUnitId, auSz, impNum), 0) auSzWonAmt,
              IF(auSz IS NULL AND rev > 0, 1, 0) paidImps,
              IF(auSz IS NULL AND evMid > 0 AND bidder LIKE '%%Clk%%', 1, 0) clicks,
              defImps,
              IFNULL(sessions, 0) sessions,
              IF(auSz IS NULL, rev, 0) rev,
              IF(auSz IS NULL, pubRev, 0) pubRev,
              IF(auSz IS NULL, eRev, 0) eRev,
              IF(auSz IS NULL, ePubRev, 0) ePubRev
            FROM
              (
                SELECT
                  s.reqId,
                  s.hour,
                  s.reqType,
                  s.prodType,
                  s.prebidSiteId,
                  s.prebidSiteName,
                  s.juiceType,
                  s.sid,
                  CASE
                    WHEN s.buAuId IS NOT NULL AND bus.id IS NOT NULL THEN s.bidder
                    WHEN s.bidder = 'N/A' THEN s.bidder
                    WHEN s.bidder = s.impBidder THEN s.impBidder
                    WHEN s.bidder = s.evBidder THEN s.evBidder
                    ELSE NULL
                  END bidder,
                  s.took,
                  s.timedOut,
                  IF(s.bidder = s.impBidder AND s.buAuId IS NULL AND bus.id IS NULL,
                    s.impMid, NULL) impMid,
                  IFNULL(impNum, -1) impNum,
                  IF(s.bidder = s.evBidder AND s.buAuId IS NULL AND bus.id IS NULL,
                    s.evMid, NULL) evMid,
                  IFNULL(evNum, -1) evNum,
                  IFNULL(bus.mid, 0) bidMid,
                  IFNULL(s.buAuId, '') adUnitId,
                  IF((s.bidder = s.impBidder OR s.bidder = s.evBidder OR
                                                         s.bidder = "N/A") AND
                         s.buAuId IS NULL AND bus.id IS NULL, NULL, auSz) auSz,
                  s.refReqCode,
                  s.refNotifyCode,
                  s.defImps,
                  s.ctry,
                  s.state,
                  s.dma,
                  s.domain,
                  s.device,
                  s.browser,
                  s.os,
                  s.clientVers,
                  s.style,
                  s.radPercent,
                  s.utmCampaign,
                  s.utmMedium,
                  s.utmSource,
                  s.utmTerm,
                  s.utmContent,
                  s.subId,
                  bus.cpm,
                  bus.won,
                  IF(((s.bidder = s.impBidder AND s.buAuId IS NULL AND bus.id IS NULL) 
                    OR s.defImps > 0) AND s.sessionStart, 1, NULL) sessions,
                  IF(s.bidder = s.impBidder AND 
                    s.buAuId IS NULL AND bus.id IS NULL, s.rev, 0) rev,
                  IF(s.bidder = s.impBidder AND
                    s.buAuId IS NULL AND bus.id IS NULL, s.pubRev, 0) pubRev,
                  IF(s.bidder = s.evBidder AND 
                    s.buAuId IS NULL AND bus.id IS NULL, s.eRev, 0) eRev,
                  IF(s.bidder = s.evBidder AND
                    s.buAuId IS NULL AND bus.id IS NULL, s.ePubRev, 0) ePubRev
                FROM
                  (
                    SELECT
                      t.reqId,
                      t.hour,
                      t.reqType,
                      t.prodType,
                      t.prebidSiteId,
                      IF(t.prebidSiteName IN ('auto', 'man'),
                                               '', t.prebidSiteName) prebidSiteName,
                      IF(t.prebidSiteName IN ('auto', 'man'),
                                               t.prebidSiteName, '') juiceType,
                      t.sid,
                      t.bidder,
                      t.took,
                      t.timedOut,
                      t.impBidder,
                      t.impMid,
                      t.impNum,
                      t.evBidder,
                      t.evMid,
                      t.evNum,
                      t.defImps,
                      t.refReqCode,
                      t.refNotifyCode,
                      t.sessionStart,
                      t.ctry,
                      t.state,
                      t.dma,
                      t.domain,
                      t.device,
                      t.browser,
                      t.os,
                      t.clientVers,
                      t.style,
                      t.radPercent,
                      t.utmCampaign,
                      t.utmMedium,
                      t.utmSource,
                      t.utmTerm,
                      t.utmContent,
                      t.subId,
                      t.auId,
                      aus.id szId,
                      CONCAT(CAST(aus.w AS STRING), 'x',
                        CAST(aus.h AS STRING), ' ', aus.mt) auSz,
                      bu.adUnitId buAuId,
                      t.rev,
                      t.pubRev,
                      t.eRev,
                      t.ePubRev,
                      bu.sizes
                    FROM
                      (
                        SELECT
                          d.reqId,
                          TIMESTAMP(DATETIME_TRUNC(DATETIME(reqTime), HOUR)) hour,
                          IFNULL(d.reqType, '') reqType,
                          IFNULL(d.udmInternal.prodType, '') prodType,
                          IFNULL(d.prebidSiteId, 0) prebidSiteId,
                          IFNULL(d.prebidSiteName, '') prebidSiteName,
                          IFNULL(d.udmInternal.sid, 0) sid,
                          br.bidder,
                          IFNULL(br.took, 0) took,
                          IF(br.timedOut IS NULL, 0, 1) timedOut,
                          i.bidder impBidder,
                          i.mid impMid,
                          impNum,
                          e.bidder evBidder,
                          e.mid evMid,
                          evNum,
                          IF(br.bidder = 'N/A',
                            ARRAY_LENGTH(d.udmInternal.defaultMids), 0) defImps,
                          d.udmInternal.refReqCode,
                          d.udmInternal.refNotifyCode,
                          d.sessionStart,
                          d.ctry,
                          d.state,
                          d.dma,
                          d.domain,
                          d.device,
                          d.browser,
                          d.os,
                          d.udmInternal.clientVers,
                          d.udmInternal.style,
                          IFNULL(d.udmInternal.radPercent, 100) radPercent,
                          d.utmCampaign,
                          d.utmMedium,
                          d.utmSource,
                          d.utmTerm,
                          d.utmContent,
                          d.subId,
                          au.id auId,
                          i.rev,
                          i.pubRev,
                          e.rev eRev,
                          e.pubRev ePubRev,
                          au.sizes,
                          br.bidUnits
                        FROM
                          rad.radDetail d
                          LEFT OUTER JOIN UNNEST(adUnits) au
                          LEFT OUTER JOIN UNNEST(bidRequests) br
                          LEFT OUTER JOIN UNNEST(imps) i WITH OFFSET AS impNum
                          LEFT OUTER JOIN UNNEST(events) e WITH OFFSET AS evNum
                        WHERE
                          d.reqTime >= TIMESTAMP("%v") AND
                          d.reqTime < TIMESTAMP("%v")
                      ) t
                      LEFT OUTER JOIN UNNEST(sizes) aus
                      LEFT OUTER JOIN UNNEST(bidUnits) bu
                        ON t.auId = bu.adUnitId
                  ) s
                  LEFT OUTER JOIN UNNEST(sizes) bus
                    ON s.szId = bus.id
              )
            WHERE
              IFNULL(bidder, '') != ''
          ) r
          LEFT OUTER JOIN rad.syncMediaTbl m
            ON CASE
                WHEN impMid > 0 THEN impMid
                WHEN evMid > 0 THEN evMid
                ELSE bidMid
              END = m.mid
        GROUP BY
          hour,
          reqType,
          prodType,
          prebidSiteId,
          prebidSiteName,
          juiceType,
          sid,
          CASE
            WHEN impMid > 0 THEN impMid
            WHEN evMid > 0 THEN evMid
            ELSE bidMid
          END,
          advName,
          adUnit,
          IF(r.adType = "", IFNULL(m.adType, ""), r.adType),
          ctry,
          state,
          dma,
          device,
          browser,
          os,
          domain,
          clientVersion,
          utmCampaign,
          utmMedium,
          utmSource,
          utmTerm,
          utmContent,
          subId,
          edgeStyle`

	hourlySummaryQry = `
        SELECT
          hour,
          reqType,
          prodType,
          prebidSiteId,
          prebidSiteName,
          sid,
          mid,
          advName,
          adUnit,
          adType,
          ctry,
          state,
          dma,
          device,
          browser,
          os,
          domain,
          utmCampaign,
          utmMedium,
          utmSource,
          utmTerm,
          utmContent,
          subId,
          edgeStyle,
          IFNULL(SUM(impRequests), 0) impRequests,
          IFNULL(SUM(refRequests), 0) refRequests,
          IFNULL(SUM(auctions), 0) auctions,
          IFNULL(SUM(bidRequests), 0) bidRequests,
          IFNULL(SUM(bids), 0) bids,
          IFNULL(SUM(bidAmt), 0) bidAmt,
          IFNULL(SUM(wonBids), 0) wonBids,
          IFNULL(SUM(wonBidAmt), 0) wonBidAmt,
          IFNULL(SUM(paidImps), 0) paidImps,
          IFNULL(SUM(refImps), 0) refImps,
          IFNULL(SUM(defImps), 0) defImps,
          IFNULL(SUM(sessions), 0) sessions,
          IFNULL(SUM(rev), 0) rev,
          IFNULL(SUM(pubRev), 0) pubRev,
          IFNULL(SUM(timeOuts), 0) timeOuts,
          IFNULL(SUM(respTook), 0) respTook,
          IFNULL(SUM(clicks), 0) clicks
        FROM
          rad.stageSummary
        WHERE
          hour >= TIMESTAMP("%v") AND
          hour < TIMESTAMP("%v")
        GROUP BY
          hour,
          reqType,
          prodType,
          prebidSiteId,
          prebidSiteName,
          sid,
          mid,
          advName,
          adUnit,
          adType,
          ctry,
          state,
          dma,
          device,
          browser,
          os,
          domain,
          utmCampaign,
          utmMedium,
          utmSource,
          utmTerm,
          utmContent,
          subId,
          edgeStyle`

	custom1SummaryQry = `
        SELECT
          s.hour,
          s.juiceType,
          s.sid,
          IFNULL(m.tid, 0) tid,
          IFNULL(m.aid, 0) aid,
          s.ctry,
          s.device,
          s.browser,
          s.os,
          IFNULL(SUM(s.impRequests), 0) impRequests,
          IFNULL(SUM(s.refRequests), 0) refRequests,
          IFNULL(SUM(s.auctions), 0) auctions,
          IFNULL(SUM(s.bidRequests), 0) bidRequests,
          IFNULL(SUM(s.bids), 0) bids,
          IFNULL(SUM(s.bidAmt), 0) bidAmt,
          IFNULL(SUM(s.wonBids), 0) wonBids,
          IFNULL(SUM(s.wonBidAmt), 0) wonBidAmt,
          IFNULL(SUM(s.paidImps), 0) paidImps,
          IFNULL(SUM(s.refImps), 0) refImps,
          IFNULL(SUM(s.defImps), 0) defImps,
          IFNULL(SUM(s.sessions), 0) sessions,
          IFNULL(SUM(s.rev), 0) rev,
          IFNULL(SUM(s.pubRev), 0) pubRev,
          IFNULL(SUM(s.timeOuts), 0) timeOuts,
          IFNULL(SUM(s.respTook), 0) respTook,
          IFNULL(SUM(s.clicks), 0) clicks
        FROM
          rad.stageSummary s
          LEFT OUTER JOIN rad.syncMediaTbl m
            ON s.mid = m.mid
        WHERE
          hour >= TIMESTAMP("%v") AND
          hour < TIMESTAMP("%v") AND
          reqType = "UDM"
        GROUP BY
          s.hour,
          s.juiceType,
          s.sid,
          IFNULL(m.tid, 0),
          IFNULL(m.aid, 0),
          s.ctry,
          s.device,
          s.browser,
          s.os`

	custom2SummaryQry = `
        SELECT
          hour,
          sid,
          utmCampaign,
          IFNULL(SUM(impRequests), 0) impRequests,
          IFNULL(SUM(refRequests), 0) refRequests,
          IFNULL(SUM(auctions), 0) auctions,
          IFNULL(SUM(bidRequests), 0) bidRequests,
          IFNULL(SUM(bids), 0) bids,
          IFNULL(SUM(bidAmt), 0) bidAmt,
          IFNULL(SUM(wonBids), 0) wonBids,
          IFNULL(SUM(wonBidAmt), 0) wonBidAmt,
          IFNULL(SUM(paidImps), 0) paidImps,
          IFNULL(SUM(refImps), 0) refImps,
          IFNULL(SUM(defImps), 0) defImps,
          IFNULL(SUM(sessions), 0) sessions,
          IFNULL(SUM(rev), 0) rev,
          IFNULL(SUM(pubRev), 0) pubRev,
          IFNULL(SUM(timeOuts), 0) timeOuts,
          IFNULL(SUM(respTook), 0) respTook,
          IFNULL(SUM(clicks), 0) clicks
        FROM
          rad.hourlySummary
        WHERE
          hour >= TIMESTAMP("%v") AND
          hour < TIMESTAMP("%v") AND
          reqType = "UDM"
        GROUP BY
          hour,
          sid,
          utmCampaign`
)

var (
	gRunStart, gHourStamp string
	gTrcCtx               logClnt.CtxT
	gLogCtx               logClnt.CtxT
)

func notify(err error, info string) {
	msg := fmt.Sprintf("Time: %v, info: %v, err: %v\n", time.Now(), info, err.Error())
	_ = uu.SendEmailMsg([]string{"dwoods@underdogmedia.com", "lisa@underdogmedia.com", "ltien@underdogmedia.com",
		"dhrebenach@underdogmedia.com", "jmiller@underdogmedia.com", "shayne.mihalka@underdogmedia.com"},
		"admin@underdogmedia.com", "LoadRadSummary", msg, "text/plain")
}

func bombErr(err error) {
	errMsg := fmt.Sprintf("Unexpected error: %v\n", err.Error())
	gLogCtx.LogToFile(errMsg)
	notify(err, "loadRadSummary bombErr")
	fmt.Printf(errMsg)
	gTrcCtx.LogToFile(fmt.Sprintf(
		"%v: loadRadSummary, start: %v, stamp: %v, bombErr: %v, exiting...\n",
		uu.GetTimeStamp(), gRunStart, gHourStamp, err.Error()))
	os.Exit(-1)
}

func appendTblFromSql(bqCtx context.Context, bqClntP *bigquery.Client,
	tblName, qry string) error {
	q := bqClntP.Query(qry)
	q.QueryConfig.UseLegacySQL = false
	q.QueryConfig.Dst = bqClntP.Dataset(rc.BQDataSet).Table(tblName)
	q.QueryConfig.CreateDisposition = bigquery.CreateIfNeeded
	q.QueryConfig.WriteDisposition = bigquery.WriteAppend

	bqJob, qErr := q.Run(bqCtx)
	if qErr != nil {
		return qErr
	}
	qStatus, sErr := bqJob.Wait(bqCtx)
	if sErr != nil {
		return sErr
	}
	return qStatus.Err()
}

func doHour(bqCtx context.Context, bqClntP *bigquery.Client,
	bqStartTs, bqEndTs string, force, clean bool) error {
	existingRows := int64(0)
	if !force || clean {
		qry := fmt.Sprintf(
			`SELECT COUNT(*) FROM rad.stageSummary
             WHERE hour >= '%v' AND hour < '%v' AND reqType = "UDM"`,
			bqStartTs, bqEndTs)
		q := bqClntP.Query(qry)
		q.QueryConfig.UseLegacySQL = false
		bqRows, err := q.Read(bqCtx)
		if err != nil {
			return (err)
		}
		var cnt []bigquery.Value
		if err = bqRows.Next(&cnt); err != nil {
			if err == iterator.Done {
				return errors.New("Empty results getting summary table count.")
			}
			return err
		}
		if len(cnt) != 1 {
			return errors.New("Malformed results getting summary table count.")
		}
		existingRows = cnt[0].(int64)
	}
	if existingRows == 0 || force {
		summariesToClear := []string{"stageSummary", "hourlySummary",
			"custom1Summary", "custom2Summary"}
		if existingRows > 0 {
			for _, summary := range summariesToClear {
				udmClause := ""
				if summary == "stageSummary" || summary == "hourlySummary" {
					udmClause = ` AND reqType = "UDM"`
				}
				dQry := fmt.Sprintf(
					`DELETE FROM rad.%v WHERE hour >= '%v' AND hour < '%v'%v`,
					summary, bqStartTs, bqEndTs, udmClause)
				dQ := bqClntP.Query(dQry)
				dQ.QueryConfig.UseLegacySQL = false
				dJob, drErr := dQ.Run(bqCtx)
				if drErr != nil {
					return drErr
				}
				dStatus, dwErr := dJob.Wait(bqCtx)
				if dwErr != nil {
					return dwErr
				}
				if dStatus.Err() != nil {
					return dStatus.Err()
				}
			}
		}
		qry := fmt.Sprintf(stageSummaryQry, bqStartTs, bqEndTs)
		qErr := appendTblFromSql(bqCtx, bqClntP, "stageSummary", qry)
		if qErr == nil {
			qry = fmt.Sprintf(hourlySummaryQry, bqStartTs, bqEndTs)
			qErr = appendTblFromSql(bqCtx, bqClntP, "hourlySummary", qry)
			if qErr == nil {
				qry = fmt.Sprintf(custom1SummaryQry, bqStartTs, bqEndTs)
				qErr = appendTblFromSql(bqCtx, bqClntP, "custom1Summary", qry)
				if qErr == nil {
					qry = fmt.Sprintf(custom2SummaryQry, bqStartTs, bqEndTs)
					qErr = appendTblFromSql(bqCtx, bqClntP, "custom2Summary", qry)
				}
			}
		}
		return qErr
	}
	return nil
}

func main() {

	gHourStamp = ""
	gRunStart = uu.GetTimeStamp()
	gTrcCtx.Init("ldr_trc.log")
	gTrcCtx.LogToFile(
		fmt.Sprintf("%v: loadRadSummary, starting...\n", gRunStart))

	gLogCtx.Init("loadRadSummary.log")
	gLogCtx.SetPrtHdr()
	gLogCtx.LogPrintf("\nStarting loadRadSummary at %v\n", time.Now())

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", rc.UdmRadalyticsAuthFile)
	var err error = nil
	bqCtx := context.Background()
	bqClntP, bqErr := bigquery.NewClient(bqCtx, rc.UdmRadalyticsProject)
	if bqErr != nil {
		bombErr(errors.New(fmt.Sprintf("bigquery.NewClient err: %v\n", bqErr.Error())))
	}

	if len(os.Args) > 3 {
		bombErr(errors.New(fmt.Sprintf("Invalid number of args: %v\n", len(os.Args))))
	}

	if len(os.Args) >= 2 {
		if len(os.Args[1]) != 10 {
			bombErr(errors.New(fmt.Sprintf("Invalid hour stamp: %v\n", os.Args[1])))
		}
		loc, _ := time.LoadLocation("America/Los_Angeles")
		startHr, tErr := time.ParseInLocation("2006010215", os.Args[1], loc)
		if tErr != nil {
			bombErr(errors.New(fmt.Sprintf("Invalid hour stamp: %v\n", os.Args[1])))
		}
		startUtc := startHr.UTC()
		endUtc := startUtc.Add(time.Hour)
		start := fmt.Sprintf("%04d-%02d-%02d %02d:00:00",
			startUtc.Year(), startUtc.Month(), startUtc.Day(), startUtc.Hour())
		end := fmt.Sprintf("%04d-%02d-%02d %02d:00:00",
			endUtc.Year(), endUtc.Month(), endUtc.Day(), endUtc.Hour())
		gHourStamp = start

		doClean := false
		if len(os.Args) > 2 {
			doClean = true
		}

		gTrcCtx.LogToFile(fmt.Sprintf(
			"%v: loadRadSummary, start: %v, run manual, cln: %v, doHour: %v\n",
			uu.GetTimeStamp(), gRunStart, doClean, gHourStamp))
		if err = doHour(bqCtx, bqClntP, start, end, true, doClean); err != nil {
			bombErr(errors.New(fmt.Sprintf(
				"Error processing command line stamp %v: %v\n",
				os.Args[1], err.Error())))
		}
	} else {

		startUtc := time.Now().UTC().Add(-1 * time.Hour)
		for hoursLeft := 24; hoursLeft > 0; hoursLeft-- {
			endUtc := startUtc.Add(time.Hour)
			force := false
			if hoursLeft == 24 {
				force = true
			}

			start := fmt.Sprintf("%04d-%02d-%02d %02d:00:00",
				startUtc.Year(), startUtc.Month(), startUtc.Day(), startUtc.Hour())
			end := fmt.Sprintf("%04d-%02d-%02d %02d:00:00",
				endUtc.Year(), endUtc.Month(), endUtc.Day(), endUtc.Hour())
			gHourStamp = start

			gTrcCtx.LogToFile(fmt.Sprintf(
				"%v: loadRadSummary, start: %v, run doHour: %v\n",
				uu.GetTimeStamp(), gRunStart, gHourStamp))

			if err = doHour(bqCtx, bqClntP, start, end, force, false); err != nil {
				bombErr(errors.New(fmt.Sprintf(
					"Error processing hour %v: %v\n", start, err.Error())))
			}
			startUtc = startUtc.Add(-1 * time.Hour)
		}
	}
	gTrcCtx.LogToFile(fmt.Sprintf(
		"%v: loadRadSummary, start: %v, exiting...\n", uu.GetTimeStamp(), gRunStart))
}
