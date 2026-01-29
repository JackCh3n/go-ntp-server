package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	Version = "1.1.0"
	Commit  = "52532ac222b6d837625b3ed5a5ee22e4fb8a1056"
	Date    = "2026-01-29"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
	maxBufferSize  = 1024
	maxTimeOffset  = 54000 * time.Second // 最大允许时间偏移54秒
)

var logger *log.Logger
var requestCounter uint64
var serverStartTime time.Time

func initLogger() {
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	logger = log.New(f, "", log.LstdFlags)
	serverStartTime = time.Now()
	logger.Printf("NTP Server v%s started at %s (UTC: %s)", Version,
		serverStartTime.Format("2006-01-02 15:04:05"),
		serverStartTime.UTC().Format("2006-01-02 15:04:05"))
	log.Printf("NTP Server v%s started at %s", Version, serverStartTime.Format("2006-01-02 15:04:05"))
	log.Printf("Server will respond as Stratum 1 with GPS clock source")
	log.Printf("Maximum allowed time offset: %v", maxTimeOffset)
}

func main() {
	initLogger()
	
	// 运行自检
	testNTPTimeConversion()
	
	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()
	
	conn.SetReadBuffer(maxBufferSize)
	conn.SetWriteBuffer(maxBufferSize)

	log.Printf("NTP server listening on %s", port)
	log.Printf("Log file: %s", logFileName)

	for {
		buf := make([]byte, 1024)
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("error reading: %v", err)
			continue
		}
		if n < 48 {
			log.Printf("received short packet from %v (size: %d)", clientAddr, n)
			continue
		}
		go handleNTPRequest(conn, clientAddr, buf[:n])
	}
}

func handleNTPRequest(conn *net.UDPConn, addr *net.UDPAddr, req []byte) {
	reqID := atomic.AddUint64(&requestCounter, 1)
	
	// 记录请求到达时间 (T2)
	requestArrivalTime := time.Now()
	t2 := requestArrivalTime.UTC()
	
	// 解析请求中的传输时间戳 (T1)
	transmitSec := binary.BigEndian.Uint32(req[40:44])
	transmitFrac := binary.BigEndian.Uint32(req[44:48])
	
	// 客户端模式检测
	mode := req[0] & 0x07
	isClientMode := (mode == 3 || mode == 1)
	
	var t1 time.Time
	if transmitSec == 0 && transmitFrac == 0 {
		if isClientMode {
			// 对于真正的客户端，设置T1为当前时间减去估计的网络延迟
			estimatedDelay := 5 * time.Millisecond
			t1 = t2.Add(-estimatedDelay)
			logger.Printf("[Req-%d] Client %s sent zero transmit timestamp, using estimated T1 (assumed %v delay)",
				reqID, addr.IP, estimatedDelay)
		} else {
			t1 = t2
		}
	} else {
		// 正常情况：从请求中解析T1
		t1 = ntpToTime(transmitSec, transmitFrac)
		
		if t1.IsZero() {
			logger.Printf("[Req-%d] Client %s sent invalid timestamp, using current time", reqID, addr.IP)
			t1 = t2.Add(-5 * time.Millisecond)
		} else {
			// 检查时间偏移是否过大
			timeDiff := t2.Sub(t1)
			if timeDiff.Abs() > maxTimeOffset {
				logger.Printf("[Req-%d] WARNING: Large time offset detected: %v (client: %s, server: %s)",
					reqID, timeDiff, t1.Format(time.RFC3339Nano), t2.Format(time.RFC3339Nano))
				logger.Printf("[Req-%d] Client %s time: %s", reqID, addr.IP, t1.Format("2006-01-02 15:04:05.000000000"))
			}
		}
	}
	
	// 使用当前时间作为服务器时间
	currentServerTime := time.Now().UTC()
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2, currentServerTime, reqID)
	
	// 发送响应
	_, err := conn.WriteToUDP(resp, addr)
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
		return
	}
	
	// 记录请求详情
	go logRequestDetails(reqID, addr, t1, t2, requestArrivalTime, currentServerTime, req[0])
	
	// 记录NTP响应包详细信息
	go logNTPPacketDetails(reqID, addr, resp, req, t1, t2, currentServerTime)
}

func buildNTPResponse(req []byte, t1, t2, responseTime time.Time, reqID uint64) []byte {
	resp := make([]byte, 48)
	
	// 保存请求头信息
	liVnMode := req[0]
	version := (liVnMode >> 3) & 0x07
	
	// 设置NTP响应头
	// LI=0 (无警告), VN=请求的版本, Mode=4 (服务器模式)
	resp[0] = ((version & 0x07) << 3) | 0x04
	
	// Stratum: 1 (一级服务器)
	resp[1] = 1
	
	// Poll: 使用请求的轮询间隔或默认值
	poll := req[2]
	if poll < 4 || poll > 15 {
		poll = 6
	}
	resp[2] = poll
	
	// Precision: -20 (约1微秒精度)
	resp[3] = 0xEC
	
	// 根延迟: 设置为0x0000.0001 (15.625微秒)
	binary.BigEndian.PutUint32(resp[4:8], 0x00000001)
	
	// 根分散: 设置为0x0000.0001 (15.625微秒)
	binary.BigEndian.PutUint32(resp[8:12], 0x00000001)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 关键修复：使用正确的参考时间（服务器启动时间）
	refTime := serverStartTime.UTC()
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 必须与请求中的传输时间戳完全一致
	copy(resp[24:32], req[40:48])
	
	// 接收时间戳 (T2) - 服务器接收时间
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 服务器发送时间
	setNTPTime(resp[40:48], responseTime)
	
	return resp
}

func logNTPPacketDetails(reqID uint64, addr *net.UDPAddr, resp []byte, req []byte, t1, t2, t3 time.Time) {
	if len(resp) < 48 {
		return
	}
	
	liVnMode := resp[0]
	li := (liVnMode >> 6) & 0x03
	version := (liVnMode >> 3) & 0x07
	mode := liVnMode & 0x07
	
	stratum := resp[1]
	poll := resp[2]
	precision := int8(resp[3])
	
	rootDelay := binary.BigEndian.Uint32(resp[4:8])
	rootDispersion := binary.BigEndian.Uint32(resp[8:12])
	refID := binary.BigEndian.Uint32(resp[12:16])
	
	refSec := binary.BigEndian.Uint32(resp[16:20])
	refFrac := binary.BigEndian.Uint32(resp[20:24])
	refTime := ntpToTime(refSec, refFrac)
	
	origSec := binary.BigEndian.Uint32(resp[24:28])
	origFrac := binary.BigEndian.Uint32(resp[28:32])
	origTime := ntpToTime(origSec, origFrac)
	
	reqTransmitSec := binary.BigEndian.Uint32(req[40:44])
	reqTransmitFrac := binary.BigEndian.Uint32(req[44:48])
	reqTransmitTime := ntpToTime(reqTransmitSec, reqTransmitFrac)
	
	recvSec := binary.BigEndian.Uint32(resp[32:36])
	recvFrac := binary.BigEndian.Uint32(resp[36:40])
	recvTime := ntpToTime(recvSec, recvFrac)
	
	transSec := binary.BigEndian.Uint32(resp[40:44])
	transFrac := binary.BigEndian.Uint32(resp[44:48])
	transTime := ntpToTime(transSec, transFrac)
	
	refIDStr := "UNKNOWN"
	if refID>>24 == 0x47 && (refID>>16)&0xFF == 0x50 && (refID>>8)&0xFF == 0x53 {
		refIDStr = "GPS"
	} else {
		refIDStr = fmt.Sprintf("0x%08X", refID)
	}
	
	var liStr string
	switch li {
	case 0:
		liStr = "0 - no warning"
	case 1:
		liStr = "1 - last minute has 61 seconds"
	case 2:
		liStr = "2 - last minute has 59 seconds"
	case 3:
		liStr = "3 - alarm condition (clock not synchronized)"
	default:
		liStr = fmt.Sprintf("%d - unknown", li)
	}
	
	var modeStr string
	switch mode {
	case 1:
		modeStr = "1 - Symmetric Active"
	case 2:
		modeStr = "2 - Symmetric Passive"
	case 3:
		modeStr = "3 - Client"
	case 4:
		modeStr = "4 - Server"
	case 5:
		modeStr = "5 - Broadcast"
	case 6:
		modeStr = "6 - Control"
	case 7:
		modeStr = "7 - Private"
	default:
		modeStr = fmt.Sprintf("%d - unknown", mode)
	}
	
	var stratumStr string
	switch stratum {
	case 0:
		stratumStr = "0 - unspecified or unavailable"
	case 1:
		stratumStr = "1 - primary reference (e.g., radio clock)"
	default:
		if stratum <= 15 {
			stratumStr = fmt.Sprintf("%d - secondary reference (via NTP)", stratum)
		} else {
			stratumStr = fmt.Sprintf("%d - reserved", stratum)
		}
	}
	
	pollInterval := 1 << uint(poll)
	
	logger.Printf("[Req-%d] NTP响应包详细信息:", reqID)
	logger.Printf("  目标地址: %s:%d", addr.IP, addr.Port)
	logger.Printf("  NTP头: LI=%s, VN=%d, Mode=%s", liStr, version, modeStr)
	logger.Printf("  Stratum: %s", stratumStr)
	logger.Printf("  Poll: %d (间隔: %d秒)", poll, pollInterval)
	logger.Printf("  Precision: %d (%.6f秒)", precision, float64(precision)*1e-9)
	logger.Printf("  Root Delay: 0x%08X (%.6f秒)", rootDelay, float64(int32(rootDelay))/65536.0)
	logger.Printf("  Root Dispersion: 0x%08X (%.6f秒)", rootDispersion, float64(int32(rootDispersion))/65536.0)
	logger.Printf("  Reference ID: %s", refIDStr)
	
	if !refTime.IsZero() {
		logger.Printf("  Reference Timestamp: %s", refTime.Format("2006-01-02 15:04:05.000000000"))
	}
	
	if !reqTransmitTime.IsZero() {
		logger.Printf("  请求中的T1 (客户端发送): %s", reqTransmitTime.Format("2006-01-02 15:04:05.000000000"))
	}
	
	if !origTime.IsZero() {
		logger.Printf("  响应中的Originate Timestamp: %s", origTime.Format("2006-01-02 15:04:05.000000000"))
		
		if origSec == reqTransmitSec && origFrac == reqTransmitFrac {
			logger.Printf("  ✓ OriginateTimestamp与请求中的TransmitTimestamp完全匹配")
		} else {
			logger.Printf("  ✗ OriginateTimestamp与请求中的TransmitTimestamp不匹配")
		}
	}
	
	if !recvTime.IsZero() {
		logger.Printf("  Receive Timestamp: %s", recvTime.Format("2006-01-02 15:04:05.000000000"))
	}
	
	if !transTime.IsZero() {
		logger.Printf("  Transmit Timestamp: %s", transTime.Format("2006-01-02 15:04:05.000000000"))
	}
	
	tolerance := time.Microsecond
	
	if !t1.IsZero() && !t2.IsZero() && !t3.IsZero() {
		if !origTime.Equal(t1) {
			diff := origTime.Sub(t1)
			if diff.Abs() > tolerance {
				logger.Printf("  警告: 响应中的原始时间戳与记录的T1不匹配! 差异: %v", diff)
			} else {
				logger.Printf("  ✓ 响应中的原始时间戳与记录的T1在允许误差范围内匹配")
			}
		} else {
			logger.Printf("  ✓ 响应中的原始时间戳与记录的T1完全匹配")
		}
		
		if !recvTime.Equal(t2) {
			diff := recvTime.Sub(t2)
			if diff.Abs() > tolerance {
				logger.Printf("  警告: 响应中的接收时间戳与记录的T2不匹配! 差异: %v", diff)
			} else {
				logger.Printf("  ✓ 响应中的接收时间戳与记录的T2在允许误差范围内匹配")
			}
		} else {
			logger.Printf("  ✓ 响应中的接收时间戳与记录的T2完全匹配")
		}
		
		if !transTime.Equal(t3) {
			diff := transTime.Sub(t3)
			if diff.Abs() > tolerance {
				logger.Printf("  警告: 响应中的传输时间戳与记录的T3不匹配! 差异: %v", diff)
			} else {
				logger.Printf("  ✓ 响应中的传输时间戳与记录的T3在允许误差范围内匹配")
			}
		} else {
			logger.Printf("  ✓ 响应中的传输时间戳与记录的T3完全匹配")
		}
	}
	
	if li != 0 {
		logger.Printf("  警告: Leap Indicator应为00(无警告)，实际为: %d", li)
	}
	
	if stratum != 1 {
		logger.Printf("  警告: Stratum应为1(一级服务器)，实际为: %d", stratum)
	}
	
	if mode != 4 {
		logger.Printf("  警告: Mode应为4(服务器模式)，实际为: %d", mode)
	}
	
	if poll < 4 || poll > 15 {
		logger.Printf("  警告: Poll应在4-15范围内，实际为: %d", poll)
	}
	
	logger.Printf("[Req-%d] NTP响应包构建完成", reqID)
}

func ntpToTime(sec uint32, frac uint32) time.Time {
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	
	var secondsSince1970 int64
	
	if sec >= 4294967295-ntpEpochOffset {
		if sec < ntpEpochOffset {
			log.Printf("WARNING: Received pre-1970 NTP timestamp: %d (1900 + %d seconds)", sec, sec)
			secondsSince1970 = int64(sec)
		} else {
			secondsSince1970 = int64(sec) - ntpEpochOffset
		}
	} else {
		secondsSince1970 = int64(sec) - ntpEpochOffset
	}
	
	nanoseconds := int64(float64(frac) * 1e9 / float64(uint64(1)<<32))
	
	if secondsSince1970 < 0 {
		if sec > 2147483647 {
			secondsSince1970 = int64(sec) - ntpEpochOffset
		}
	}
	
	if secondsSince1970 < 0 {
		return time.Time{}
	}
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

func setNTPTime(b []byte, t time.Time) {
	if t.IsZero() {
		binary.BigEndian.PutUint32(b[0:4], 0)
		binary.BigEndian.PutUint32(b[4:8], 0)
		return
	}
	
	unixTime := t.Unix()
	if unixTime < 0 {
		unixTime = 0
	}
	secondsSince1900 := uint64(unixTime) + ntpEpochOffset
	
	frac := uint64(float64(t.Nanosecond()) * float64(uint64(1)<<32) / 1e9)
	
	binary.BigEndian.PutUint32(b[0:4], uint32(secondsSince1900))
	binary.BigEndian.PutUint32(b[4:8], uint32(frac))
}

func logRequestDetails(reqID uint64, addr *net.UDPAddr, t1, t2, requestArrival, processTime time.Time, clientMode byte) {
	ip := addr.IP.String()
	
	mode := clientMode & 0x07
	version := (clientMode >> 3) & 0x07
	
	var modeStr string
	switch mode {
	case 1:
		modeStr = "Symmetric Active"
	case 2:
		modeStr = "Symmetric Passive"
	case 3:
		modeStr = "Client"
	case 4:
		modeStr = "Server"
	case 5:
		modeStr = "Broadcast"
	case 6:
		modeStr = "Control"
	case 7:
		modeStr = "Private"
	default:
		modeStr = "Reserved"
	}
	
	var timeDiff string
	var timeDiffSeconds float64
	if !t1.IsZero() {
		diff := t2.Sub(t1)
		timeDiffSeconds = diff.Seconds()
		if diff < 0 {
			timeDiff = "-" + (-diff).String()
		} else {
			timeDiff = diff.String()
		}
	}
	
	logger.Printf("=== NTP请求详情 [Req-%d] ===", reqID)
	logger.Printf("客户端: %s:%d", ip, addr.Port)
	logger.Printf("NTP版本: %d, 模式: %s", version, modeStr)
	
	if !t1.IsZero() {
		logger.Printf("T1 (客户端发送时间):  %s", t1.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("T2 (服务器接收时间):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("时间差 (T2-T1):   %s", timeDiff)
		
		t1ToT2 := t2.Sub(t1)
		processingDelay := processTime.Sub(requestArrival)
		
		logger.Printf("T1→T2延迟:       %v", t1ToT2)
		logger.Printf("处理延迟:         %v", processingDelay)
		
		clockOffset := (t2.Sub(t1) + processTime.Sub(requestArrival)) / 2
		logger.Printf("时钟偏移估计:     %v", clockOffset)
		
		clientClockOffset := t1.Sub(t2)
		logger.Printf("客户端时钟偏移:   %v (正值表示客户端比服务器快)", clientClockOffset)
		
		if timeDiffSeconds > 3600 {
			logger.Printf("警告: 客户端与服务器时间差超过1小时: %.2f小时", timeDiffSeconds/3600)
		} else if timeDiffSeconds > 60 {
			logger.Printf("注意: 客户端与服务器时间差超过1分钟: %.2f分钟", timeDiffSeconds/60)
		}
		
		if timeDiffSeconds > maxTimeOffset.Seconds() {
			logger.Printf("严重: 时间差超过最大允许值(%v)! 需要手动同步", maxTimeOffset)
		}
	} else {
		logger.Printf("T1: (未设置或为0)")
		logger.Printf("T2 (服务器接收):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
	}
	
	logger.Printf("===================\n")
}

func testNTPTimeConversion() {
	log.Println("测试NTP时间转换...")
	
	now := time.Now().UTC()
	log.Printf("当前服务器时间: %s", now.Format("2006-01-02 15:04:05.000000000"))
	log.Printf("服务器启动时间: %s", serverStartTime.Format("2006-01-02 15:04:05.000000000"))
	
	var buf [8]byte
	setNTPTime(buf[:], now)
	
	sec := binary.BigEndian.Uint32(buf[0:4])
	frac := binary.BigEndian.Uint32(buf[4:8])
	converted := ntpToTime(sec, frac)
	
	diff := converted.Sub(now)
	if diff.Abs() > time.Microsecond {
		log.Printf("警告: 时间转换误差: %v (允许范围: 1微秒)", diff)
	} else {
		log.Println("时间转换测试通过")
	}
	
	zeroTime := time.Time{}
	setNTPTime(buf[:], zeroTime)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	if sec != 0 || frac != 0 {
		log.Printf("错误: 零时间转换失败: sec=%d, frac=%d", sec, frac)
	} else {
		log.Println("零时间转换测试通过")
	}
	
	testTime := time.Date(2026, 1, 29, 9, 0, 0, 0, time.UTC)
	log.Printf("测试时间: %s", testTime.Format("2006-01-02 15:04:05.000000000"))
	setNTPTime(buf[:], testTime)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	convertedTest := ntpToTime(sec, frac)
	
	if !convertedTest.Equal(testTime) {
		log.Printf("警告: 测试时间转换差异: 期望 %v, 得到 %v, 差异: %v",
			testTime, convertedTest, convertedTest.Sub(testTime))
	} else {
		log.Println("测试时间转换测试通过")
	}
	
	log.Println("时间转换测试完成")
	
	req := make([]byte, 48)
	req[0] = 0x1B
	req[2] = 6
	
	reqT1 := time.Now().UTC().Add(-100 * time.Millisecond)
	setNTPTime(req[40:48], reqT1)
	
	t2 := time.Now().UTC()
	t3 := time.Now().UTC().Add(1 * time.Millisecond)
	
	reqTransmitSec := binary.BigEndian.Uint32(req[40:44])
	reqTransmitFrac := binary.BigEndian.Uint32(req[44:48])
	reqTransmitTime := ntpToTime(reqTransmitSec, reqTransmitFrac)
	
	resp := buildNTPResponse(req, reqTransmitTime, t2, t3, 999)
	
	if len(resp) != 48 {
		log.Printf("错误: 响应包长度错误: %d (期望48)", len(resp))
	} else {
		respLiVnMode := resp[0]
		respLi := (respLiVnMode >> 6) & 0x03
		respVersion := (respLiVnMode >> 3) & 0x07
		respMode := respLiVnMode & 0x07
		
		if respLi != 0 {
			log.Printf("警告: 响应包LI字段应为0, 实际为: %d", respLi)
		}
		if respVersion != 3 {
			log.Printf("警告: 响应包Version字段应为3, 实际为: %d", respVersion)
		}
		if respMode != 4 {
			log.Printf("错误: 响应包Mode字段应为4(Server), 实际为: %d", respMode)
		}
		
		respStratum := resp[1]
		if respStratum != 1 {
			log.Printf("警告: 响应包Stratum字段应为1, 实际为: %d", respStratum)
		}
		
		respPoll := resp[2]
		if respPoll < 4 || respPoll > 15 {
			log.Printf("警告: 响应包Poll字段应在4-15范围内, 实际为: %d", respPoll)
		}
		
		respOrigSec := binary.BigEndian.Uint32(resp[24:28])
		respOrigFrac := binary.BigEndian.Uint32(resp[28:32])
		
		if respOrigSec == reqTransmitSec && respOrigFrac == reqTransmitFrac {
			log.Println("✓ OriginateTimestamp与请求中的TransmitTimestamp完全一致")
		} else {
			log.Printf("错误: OriginateTimestamp不匹配! 响应: sec=0x%08X, frac=0x%08X, 请求: sec=0x%08X, frac=0x%08X",
				respOrigSec, respOrigFrac, reqTransmitSec, reqTransmitFrac)
		}
		
		log.Println("NTP响应包构建测试完成")
	}
}
