package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	Version = "1.0.0"
	Commit  = "d62065c2d4f7de8aae08576911db8b171c915a7c"
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
		// 客户端会根据服务器的T2、T3和原始T1计算时间偏移
		if t1.IsZero() {
			// 无效的时间戳（如1970年之前）
			logger.Printf("[Req-%d] Client %s sent invalid timestamp, using current time", reqID, addr.IP)
			t1 = t2.Add(-5 * time.Millisecond)
		} else {
			// 记录时间戳，但不调整它
			// 这是NTP协议的核心：服务器原样返回客户端的T1
			logger.Printf("[Req-%d] Client %s sent T1=%s (UTC)", 
				reqID, addr.IP, t1.Format("2006-01-02 15:04:05.000000000"))
		}
	}
	
	// 记录请求解析完成时间
	processTime := time.Now().UTC()
	
	// 修复：TransmitTimestamp应该尽可能接近实际发送时间
	// 获取发送前的时间作为TransmitTimestamp
	t3 := time.Now().UTC()
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2, t3, reqID)
	
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
}

func buildNTPResponse(req []byte, t1, t2, t3 time.Time, reqID uint64) []byte {
	resp := make([]byte, 48)
	
	// 保存请求头信息
	liVnMode := req[0]
	version := (liVnMode >> 3) & 0x07
	
	// 设置NTP响应头
	// LI=0 (无警告), VN=请求的版本, Mode=4 (服务器模式)
	resp[0] = 0x1C | ((version & 0x07) << 3) // LI=00, VN=请求的版本, Mode=4
	// 注意：这里修复了LI（Leap Indicator）的设置，应该是00（无警告）
	
	// Stratum: 1 (一级服务器，表示与GPS等原子钟同步)
	resp[1] = 1
	
	// Poll: 使用请求的轮询间隔或默认值6 (64秒)
	poll := req[2]
	if poll < 4 || poll > 17 {
		poll = 6 // 默认值
	}
	resp[2] = poll
	
	// Precision: -20 (约1微秒精度) - 对应-6 (2^-20 ≈ 1微秒)
	resp[3] = 0xEC // -20的二进制补码表示
	
	// 根延迟: 设置为0x0001 (0.015625 ms)，合理的根延迟
	binary.BigEndian.PutUint32(resp[4:8], 0x00010000)
	
	// 根分散: 设置为0x0001 (0.015625 ms)，合理的根分散
	binary.BigEndian.PutUint32(resp[8:12], 0x00010000)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 参考时间戳: 使用服务器启动时间
	refTime := serverStartTime.UTC()
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 必须与请求中的传输时间戳一致
	setNTPTime(resp[24:32], t1)
	
	// 接收时间戳 (T2) - 服务器接收时间
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 服务器发送时间
	setNTPTime(resp[40:48], t3)
	
	return resp
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
}
