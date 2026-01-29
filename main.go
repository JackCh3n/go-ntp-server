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
	Version = "1.0.0"
	Commit  = "52532ac222b6d837625b3ed5a5ee22e4fb8a1056"
	Date    = "2026-01-29"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
	maxBufferSize  = 1024
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
}

func main() {
	initLogger()
	
	// 运行自检
	testNTPTimeConversion()
	
	log.Printf("Starting NTP Server v%s (commit: %s, built: %s)", Version, Commit, Date)
	log.Printf("Server UTC time: %s", time.Now().UTC().Format("2006-01-02 15:04:05.000000000"))
	log.Printf("Server will respond as Stratum 1 (Primary Reference) with GPS clock source")

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()
	
	// 设置更大的缓冲区
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
	// 生成请求ID用于跟踪
	reqID := atomic.AddUint64(&requestCounter, 1)
	
	// 记录请求到达时间 (T2)
	requestArrivalTime := time.Now()
	t2 := requestArrivalTime.UTC()
	
	// 解析请求中的传输时间戳 (T1)
	transmitSec := binary.BigEndian.Uint32(req[40:44])
	transmitFrac := binary.BigEndian.Uint32(req[44:48])
	
	// 客户端模式检测
	mode := req[0] & 0x07
	isClientMode := (mode == 3 || mode == 1) // Mode 3 = Client, Mode 1 = Symmetric Active
	
	var t1 time.Time
	if transmitSec == 0 && transmitFrac == 0 {
		// 客户端发送了零时间戳（某些客户端实现）
		if isClientMode {
			// 对于真正的客户端，设置T1为当前时间减去估计的网络延迟
			estimatedDelay := 5 * time.Millisecond
			t1 = t2.Add(-estimatedDelay)
			logger.Printf("[Req-%d] Client %s sent zero transmit timestamp, using estimated T1 (assumed %v delay)", 
				reqID, addr.IP, estimatedDelay)
		} else {
			// 非客户端模式，使用当前时间
			t1 = t2
		}
	} else {
		// 正常情况：从请求中解析T1
		t1 = ntpToTime(transmitSec, transmitFrac)
		
		// 核心修复：NTP服务器应信任客户端提供的时间戳T1
		if t1.IsZero() {
			// 无效的时间戳（如1970年之前）
			logger.Printf("[Req-%d] Client %s sent invalid timestamp, using current time", reqID, addr.IP)
			t1 = t2.Add(-5 * time.Millisecond)
		} else {
			// 记录时间戳，但不调整它
			logger.Printf("[Req-%d] Client %s sent T1=%s (UTC)", 
				reqID, addr.IP, t1.Format("2006-01-02 15:04:05.000000000"))
		}
	}
	
	// 记录请求解析完成时间
	processTime := time.Now().UTC()
	
	// 构建响应 - 使用原始的传输时间戳字节
	resp := buildNTPResponse(req, t1, t2, processTime, reqID)
	
	// 记录发送开始时间
	sendStart := time.Now().UTC()
	
	// 发送响应
	_, err := conn.WriteToUDP(resp, addr)
	sendEnd := time.Now().UTC()
	
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
		return
	}
	
	// 记录详细的时间戳信息
	go logRequestDetails(reqID, addr, t1, t2, requestArrivalTime, processTime, sendStart, sendEnd, req[0])
	
	// 记录NTP响应包详细信息
	go logNTPPacketDetails(reqID, addr, resp, req, t1, t2, processTime)
}

func buildNTPResponse(req []byte, t1, t2, responseTime time.Time, reqID uint64) []byte {
	resp := make([]byte, 48)
	
	// 保存请求头信息
	liVnMode := req[0]
	version := (liVnMode >> 3) & 0x07
	
	// 设置NTP响应头
	// LI=0 (无警告), VN=请求的版本, Mode=4 (服务器模式)
	// 修复：确保LI设置为00（无警告）
	resp[0] = ((version & 0x07) << 3) | 0x04 // LI=00, VN=请求的版本, Mode=4
	
	// Stratum: 1 (一级服务器，表示与GPS等原子钟同步)
	resp[1] = 1
	
	// Poll: 使用请求的轮询间隔或默认值6 (64秒)
	// 修复：确保Poll值在有效范围内（4-15，对应16-32768秒）
	poll := req[2]
	// NTP协议规定：Poll值在4-15范围内（对应16-32768秒）
	// 如果值不在范围内，使用默认值6（64秒）
	if poll < 4 || poll > 15 {
		poll = 6 // 默认值，64秒
	}
	resp[2] = poll
	
	// Precision: -20 (约1微秒精度) - 对应-20 (2^-20 ≈ 0.954微秒)
	// 修复：使用正确的精度值-20（0xEC）
	resp[3] = 0xEC // -20的二进制补码表示
	
	// 根延迟: 设置为0x0000.0001 (15.625微秒)，合理的根延迟
	// 修复：设置为更合理的值0x0000.0001而不是0x0001.0000
	binary.BigEndian.PutUint32(resp[4:8], 0x00000001)
	
	// 根分散: 设置为0x0000.0001 (15.625微秒)，合理的根分散
	// 修复：设置为更合理的值0x0000.0001而不是0x0001.0000
	binary.BigEndian.PutUint32(resp[8:12], 0x00000001)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 参考时间戳: 使用服务器启动时间
	// 修复：使用更合理的参考时间（当前时间减去1秒）
	refTime := time.Now().UTC().Add(-1 * time.Second)
	setNTPTime(resp[16:24], refTime)
	
	// 关键修复：直接复制客户端的传输时间戳字节，确保完全一致
	// 原始时间戳 (T1) - 必须与请求中的传输时间戳完全一致
	// 不再使用setNTPTime函数，而是直接复制字节
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
	
	// 解析NTP响应包信息
	liVnMode := resp[0]
	li := (liVnMode >> 6) & 0x03
	version := (liVnMode >> 3) & 0x07
	mode := liVnMode & 0x07
	
	stratum := resp[1]
	poll := resp[2]
	precision := int8(resp[3]) // 有符号整数
	
	rootDelay := binary.BigEndian.Uint32(resp[4:8])
	rootDispersion := binary.BigEndian.Uint32(resp[8:12])
	
	refID := binary.BigEndian.Uint32(resp[12:16])
	
	// 解析参考时间戳
	refSec := binary.BigEndian.Uint32(resp[16:20])
	refFrac := binary.BigEndian.Uint32(resp[20:24])
	refTime := ntpToTime(refSec, refFrac)
	
	// 解析原始时间戳
	origSec := binary.BigEndian.Uint32(resp[24:28])
	origFrac := binary.BigEndian.Uint32(resp[28:32])
	origTime := ntpToTime(origSec, origFrac)
	
	// 解析请求中的传输时间戳
	reqTransmitSec := binary.BigEndian.Uint32(req[40:44])
	reqTransmitFrac := binary.BigEndian.Uint32(req[44:48])
	reqTransmitTime := ntpToTime(reqTransmitSec, reqTransmitFrac)
	
	// 解析接收时间戳
	recvSec := binary.BigEndian.Uint32(resp[32:36])
	recvFrac := binary.BigEndian.Uint32(resp[36:40])
	recvTime := ntpToTime(recvSec, recvFrac)
	
	// 解析传输时间戳
	transSec := binary.BigEndian.Uint32(resp[40:44])
	transFrac := binary.BigEndian.Uint32(resp[44:48])
	transTime := ntpToTime(transSec, transFrac)
	
	// 构建参考ID字符串
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
	
	// 计算Poll间隔（秒）
	pollInterval := 1 << uint(poll) // 2^poll 秒
	
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
	} else {
		logger.Printf("  Reference Timestamp: 0x%08X%08X", refSec, refFrac)
	}
	
	if !reqTransmitTime.IsZero() {
		logger.Printf("  请求中的T1 (客户端发送): %s", reqTransmitTime.Format("2006-01-02 15:04:05.000000000"))
	}
	
	if !origTime.IsZero() {
		logger.Printf("  响应中的Originate Timestamp: %s", origTime.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("  响应中的Originate Timestamp原始字节: sec=0x%08X, frac=0x%08X", origSec, origFrac)
		logger.Printf("  请求中的Transmit Timestamp原始字节: sec=0x%08X, frac=0x%08X", reqTransmitSec, reqTransmitFrac)
		
		// 检查原始时间戳是否匹配
		if origSec == reqTransmitSec && origFrac == reqTransmitFrac {
			logger.Printf("  ✓ OriginateTimestamp与请求中的TransmitTimestamp完全匹配")
		} else {
			logger.Printf("  ✗ OriginateTimestamp与请求中的TransmitTimestamp不匹配")
		}
	} else {
		logger.Printf("  Originate Timestamp: 0x%08X%08X", origSec, origFrac)
	}
	
	if !recvTime.IsZero() {
		logger.Printf("  Receive Timestamp: %s", recvTime.Format("2006-01-02 15:04:05.000000000"))
	} else {
		logger.Printf("  Receive Timestamp: 0x%08X%08X", recvSec, recvFrac)
	}
	
	if !transTime.IsZero() {
		logger.Printf("  Transmit Timestamp: %s", transTime.Format("2006-01-02 15:04:05.000000000"))
	} else {
		logger.Printf("  Transmit Timestamp: 0x%08X%08X", transSec, transFrac)
	}
	
	// 验证时间戳关系 - 允许微小误差
	if !t1.IsZero() && !t2.IsZero() && !t3.IsZero() {
		// 允许1微秒的误差
		tolerance := time.Microsecond
		
		// 检查T1是否匹配
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
		
		// 检查T2是否匹配
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
		
		// 检查T3是否匹配
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
	
	// 检查响应包是否满足NTP协议要求
	// 1. 检查Leap Indicator是否为00
	if li != 0 {
		logger.Printf("  警告: Leap Indicator应为00(无警告)，实际为: %d", li)
	}
	
	// 2. 检查Stratum是否为1
	if stratum != 1 {
		logger.Printf("  警告: Stratum应为1(一级服务器)，实际为: %d", stratum)
	}
	
	// 3. 检查Mode是否为4(服务器模式)
	if mode != 4 {
		logger.Printf("  警告: Mode应为4(服务器模式)，实际为: %d", mode)
	}
	
	// 4. 检查Poll是否在有效范围内
	if poll < 4 || poll > 15 {
		logger.Printf("  警告: Poll应在4-15范围内，实际为: %d", poll)
	}
	
	logger.Printf("[Req-%d] NTP响应包构建完成", reqID)
}

// NTP时间转换函数
func ntpToTime(sec uint32, frac uint32) time.Time {
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	
	// 计算从1970年1月1日开始的秒数
	var secondsSince1970 int64
	
	// 处理2036年问题
	if sec >= 4294967295-ntpEpochOffset { // 接近溢出
		// 对于未来时间，使用更复杂的计算
		if sec < ntpEpochOffset {
			// 如果sec小于ntpEpochOffset，说明是1900-1970年间的时间
			log.Printf("WARNING: Received pre-1970 NTP timestamp: %d (1900 + %d seconds)", sec, sec)
			// 将其视为1970年之后的时间
			secondsSince1970 = int64(sec)
		} else {
			secondsSince1970 = int64(sec) - ntpEpochOffset
		}
	} else {
		secondsSince1970 = int64(sec) - ntpEpochOffset
	}
	
	// 将分数部分转换为纳秒
	// 使用浮点数计算以获得更高精度
	nanoseconds := int64(float64(frac) * 1e9 / float64(uint64(1)<<32))
	
	// 额外的验证
	if secondsSince1970 < 0 {
		// 时间戳在1970年之前
		log.Printf("WARNING: NTP timestamp before 1970: sec=%d, frac=%d, secondsSince1970=%d", sec, frac, secondsSince1970)
		
		// 尝试修复：如果sec很大但减去ntpEpochOffset后为负，可能是溢出的时间
		if sec > 2147483647 { // 超过2036年
			// 使用更复杂的时间计算
			secondsSince1970 = int64(sec) - ntpEpochOffset
		}
	}
	
	// 如果仍然为负，返回零时间
	if secondsSince1970 < 0 {
		return time.Time{}
	}
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

// 设置NTP时间戳
func setNTPTime(b []byte, t time.Time) {
	if t.IsZero() {
		binary.BigEndian.PutUint32(b[0:4], 0)
		binary.BigEndian.PutUint32(b[4:8], 0)
		return
	}
	
	// 计算从1900年1月1日开始的秒数
	unixTime := t.Unix()
	if unixTime < 0 {
		// 处理1970年之前的时间
		unixTime = 0
	}
	secondsSince1900 := uint64(unixTime) + ntpEpochOffset
	
	// 将纳秒转换为NTP分数部分
	// 分数 = (纳秒 * 2^32) / 1e9
	frac := uint64(float64(t.Nanosecond()) * float64(uint64(1)<<32) / 1e9)
	
	// 写入秒数部分
	binary.BigEndian.PutUint32(b[0:4], uint32(secondsSince1900))
	
	// 写入分数部分
	binary.BigEndian.PutUint32(b[4:8], uint32(frac))
}

func logRequestDetails(reqID uint64, addr *net.UDPAddr, t1, t2, requestArrival, processTime, sendStart, sendEnd time.Time, clientMode byte) {
	ip := addr.IP.String()
	
	// 尝试解析客户端信息
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
	
	// 计算时间差
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
		
		// 计算延迟
		t1ToT2 := t2.Sub(t1)
		processingDelay := processTime.Sub(requestArrival)
		networkSendDelay := sendEnd.Sub(sendStart)
		totalDelay := sendEnd.Sub(requestArrival)
		
		logger.Printf("T1→T2延迟:       %v", t1ToT2)
		logger.Printf("处理延迟:         %v", processingDelay)
		logger.Printf("网络发送延迟:     %v", networkSendDelay)
		logger.Printf("总处理时间:       %v", totalDelay)
		
		// 计算预期的往返延迟
		estimatedRTT := t1ToT2 + networkSendDelay
		logger.Printf("预期的往返延迟:  %v (估计)", estimatedRTT)
		
		// 计算时钟偏移
		clockOffset := (t2.Sub(t1) + sendStart.Sub(sendEnd)) / 2
		logger.Printf("时钟偏移估计:     %v", clockOffset)
		
		// 重要：显示服务器如何看待客户端的时间
		clientClockOffset := t1.Sub(t2)
		logger.Printf("客户端时钟偏移:   %v (正值表示客户端比服务器快)", clientClockOffset)
		
		// 记录时间差绝对值，帮助诊断
		if timeDiffSeconds > 3600 { // 超过1小时
			logger.Printf("警告: 客户端与服务器时间差超过1小时: %.2f小时", timeDiffSeconds/3600)
		} else if timeDiffSeconds > 60 { // 超过1分钟
			logger.Printf("注意: 客户端与服务器时间差超过1分钟: %.2f分钟", timeDiffSeconds/60)
		}
	} else {
		logger.Printf("T1: (未设置或为0)")
		logger.Printf("T2 (服务器接收):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
	}
	
	logger.Printf("===================\n")
}

// 测试NTP时间转换
func testNTPTimeConversion() {
	log.Println("测试NTP时间转换...")
	
	// 测试1: 当前时间
	now := time.Now().UTC()
	log.Printf("当前时间: %s", now.Format("2006-01-02 15:04:05.000000000"))
	
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
	
	// 测试2: 零时间
	zeroTime := time.Time{}
	setNTPTime(buf[:], zeroTime)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	if sec != 0 || frac != 0 {
		log.Printf("错误: 零时间转换失败: sec=%d, frac=%d", sec, frac)
	} else {
		log.Println("零时间转换测试通过")
	}
	
	// 测试3: 特定时间 - 修复问题中的时间
	testTime1 := time.Date(2026, 1, 28, 22, 31, 39, 212744299, time.UTC)
	log.Printf("测试时间1 (客户端时间): %s", testTime1.Format("2006-01-02 15:04:05.000000000"))
	setNTPTime(buf[:], testTime1)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	converted1 := ntpToTime(sec, frac)
	
	if !converted1.Equal(testTime1) {
		log.Printf("警告: 特定时间转换差异: 期望 %v, 得到 %v, 差异: %v", 
			testTime1, converted1, converted1.Sub(testTime1))
	} else {
		log.Println("特定时间1转换测试通过")
	}
	
	// 测试4: 服务器时间
	testTime2 := time.Date(2026, 1, 29, 6, 38, 45, 581648700, time.UTC)
	log.Printf("测试时间2 (服务器时间): %s", testTime2.Format("2006-01-02 15:04:05.000000000"))
	setNTPTime(buf[:], testTime2)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	converted2 := ntpToTime(sec, frac)
	
	if !converted2.Equal(testTime2) {
		log.Printf("警告: 特定时间转换差异: 期望 %v, 得到 %v, 差异: %v", 
			testTime2, converted2, converted2.Sub(testTime2))
	} else {
		log.Println("特定时间2转换测试通过")
	}
	
	// 测试5: 1970年之前的时间
	testTime3 := time.Date(1969, 7, 20, 20, 17, 40, 0, time.UTC) // 阿波罗11号登月
	setNTPTime(buf[:], testTime3)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	converted3 := ntpToTime(sec, frac)
	
	if !converted3.IsZero() {
		log.Printf("警告: 1970年前时间应返回零时间, 得到: %v", converted3)
	} else {
		log.Println("1970年前时间处理测试通过")
	}
	
	// 测试6: 2036年之后的时间 (NTP时间戳溢出)
	testTime4 := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC) // 2038年问题
	setNTPTime(buf[:], testTime4)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	converted4 := ntpToTime(sec, frac)
	
	// 检查转换是否合理
	if converted4.Year() != 2038 {
		log.Printf("警告: 2038年时间转换可能有问题, 得到年份: %d", converted4.Year())
	} else {
		log.Printf("2038年时间转换测试: 期望年份2038, 得到年份: %d", converted4.Year())
	}
	
	log.Println("所有时间转换测试完成")
	
	// 测试7: NTP响应包构建测试
	log.Println("测试NTP响应包构建...")
	req := make([]byte, 48)
	req[0] = 0x1B // LI=0, VN=3, Mode=3 (Client)
	req[2] = 6    // Poll=6 (64秒)
	
	// 设置请求中的传输时间戳
	reqT1 := time.Now().UTC().Add(-100 * time.Millisecond)
	setNTPTime(req[40:48], reqT1)
	
	t2 := time.Now().UTC()
	t3 := time.Now().UTC().Add(1 * time.Millisecond)
	
	// 解析请求中的T1
	reqTransmitSec := binary.BigEndian.Uint32(req[40:44])
	reqTransmitFrac := binary.BigEndian.Uint32(req[44:48])
	reqTransmitTime := ntpToTime(reqTransmitSec, reqTransmitFrac)
	
	resp := buildNTPResponse(req, reqTransmitTime, t2, t3, 999)
	
	// 验证响应包格式
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
		
		// 检查OriginateTimestamp是否与请求中的TransmitTimestamp完全一致
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
